import os
from pathlib import Path
import time
from datetime import datetime, timedelta

from sklearn.linear_model import LinearRegression
from feast import FeatureStore
import joblib

from google.cloud import bigquery

PROJECT_NAME = "integral-server-329913"
BUCKET_NAME = "feast_fraud_bkt"
BIGQUERY_DATASET_NAME = "feast_fraud_ds"
AI_PLATFORM_MODEL_NAME = "feast_fraud_mdl"


def generate_user_count_features(aggregation_end_date):
    table_id  = f"{PROJECT_NAME}.{BIGQUERY_DATASET_NAME}.user_count_transactions_7d"

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition='WRITE_APPEND')

    aggregation_start_date = datetime.now() - timedelta(days=7)

    sql = f"""
    SELECT
        src_account AS user_id,
        COUNT(*) AS transaction_count_7d,
        timestamp'{aggregation_end_date.isoformat()}' AS feature_timestamp
    FROM
        feast-oss.fraud_tutorial.transactions
    WHERE
        timestamp BETWEEN TIMESTAMP('{aggregation_start_date.isoformat()}')
        AND TIMESTAMP('{aggregation_end_date.isoformat()}')
    GROUP BY
        user_id
    """

    query_job = client.query(sql, job_config=job_config)
    query_job.result()
    print(f"Generated features as of {aggregation_end_date.isoformat()}")


def backfill_features(earliest_aggregation_end_date, interval, num_iterations):
    aggregation_end_date = earliest_aggregation_end_date
    for _ in range(num_iterations):
        generate_user_count_features(aggregation_end_date=aggregation_end_date)
        time.sleep(1)
        aggregation_end_date += interval


def predict(model, store, entity_rows):
    feature_vector = store.get_online_features(
        features=[
            "user_transaction_count_7d:transaction_count_7d",
            "user_account_features:credit_score",
            "user_account_features:account_age_days",
            "user_account_features:user_has_2fa_installed",
            "user_has_fraudulent_transactions:user_has_fraudulent_transactions_7d"
        ],
        entity_rows=entity_rows
    ).to_dict()

    # Delete entity keys
    del feature_vector["user_id"]

    # Flatten response from Feast
    instances = [
        [feature_values[i] for feature_values in feature_vector.values()]
        for i in range(len(entity_rows))
    ]

    return model.predict(instances)

    # service = googleapiclient.discovery.build('ml', 'v1')
    # name = f'projects/{PROJECT_ID}/models/{AI_PLATFORM_MODEL_NAME}'

    # response = service.projects().predict(
    #     name=name,
    #     body={'instances': instances}
    # ).execute()
    #
    # clear_output()
    # return response


if __name__ == "__main__":

    cur_dir_path = Path(__file__).absolute().parent

    # os.system(f"gsutil mb gs://{BUCKET_NAME}")
    # os.system(f"bq mk {BIGQUERY_DATASET_NAME}")

    # backfill_features(
    #     earliest_aggregation_end_date=datetime.now() - timedelta(days=7), interval=timedelta(days=1), num_iterations=8)

    # Initialize a FeatureStore with our current repository's configurations
    store = FeatureStore(repo_path=str(cur_dir_path.parent.parent / "repos/fraud_bigquery_repo"))

    # Get training data
    now = datetime.now()
    two_days_ago = datetime.now() - timedelta(days=2)

    training_data = store.get_historical_features(
        entity_df=f"""
        select 
            src_account as user_id,
            timestamp,
            is_fraud
        from
            feast-oss.fraud_tutorial.transactions
        where
            timestamp between timestamp('{two_days_ago.isoformat()}') 
            and timestamp('{now.isoformat()}')""",
        features=[
            "user_transaction_count_7d:transaction_count_7d",
            "user_account_features:credit_score",
            "user_account_features:account_age_days",
            "user_account_features:user_has_2fa_installed",
            "user_has_fraudulent_transactions:user_has_fraudulent_transactions_7d"
        ],
        full_feature_names=True
    ).to_df()

    # print(training_data.head())


    # Drop stray nulls
    training_data.dropna(inplace=True)

    # Select training matrices
    X = training_data[[
        "user_transaction_count_7d__transaction_count_7d",
        "user_account_features__credit_score",
        "user_account_features__account_age_days",
        "user_account_features__user_has_2fa_installed",
        "user_has_fraudulent_transactions__user_has_fraudulent_transactions_7d"
    ]]
    y = training_data["is_fraud"]

    # Train a simple SVC model
    model = LinearRegression()
    model.fit(X, y)

    # Get first two rows of training data

    # Make a test prediction
    joblib.dump(model, "model.joblib")
    print("prediction: ")
    print(predict(model, store, [{"user_id": "v5zlw0"}]))
