from typing import Optional
from pathlib import Path
import time
from datetime import datetime, timedelta

from sqlalchemy import create_engine, insert, select

from sklearn.linear_model import LinearRegression

from feast import FeatureStore

import joblib

from google.cloud import bigquery

from feast_sandbox.utils import DataSourceType


PROJECT_NAME = "integral-server-329913"
BUCKET_NAME = "feast_fraud_bkt"
BIGQUERY_DATASET_NAME = "feast_fraud_ds"
AI_PLATFORM_MODEL_NAME = "feast_fraud_mdl"


def generate_user_count_features(aggregation_end_date):
    table_id = f"{PROJECT_NAME}.{BIGQUERY_DATASET_NAME}.user_count_transactions_7d"

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


def get_last_timestamp(table_name: str, field_name: str, conn_str: str) -> Optional[datetime]:
    """
    Returns latest timestamp from given table (by given field)
    """
    engine = create_engine(conn_str)
    conn = engine.connect()

    rows = conn.execute(f"SELECT {field_name} FROM {table_name} ORDER BY {field_name} DESC LIMIT 1")
    results = rows.all()
    if not results:
        return None
    last_datetime = results[0][0]
    return last_datetime


def process_fraud_tutorial(datasource_type: DataSourceType):
    cur_dir_path = Path(__file__).absolute().parent

    # Let's select all transactions for two last days back from last transaction..
    if datasource_type == DataSourceType.PARQUET:
        raise NotImplementedError("PARQIET repo is not supported yet for this tutorial")
    elif datasource_type == DataSourceType.BIGQUERY:
        last_timestamp = get_last_timestamp("feast-oss.fraud_tutorial.transactions", "timestamp",
                                            f"bigquery://{PROJECT_NAME}")
        entity_df_selection_str = f"""
            select src_account as user_id, timestamp, is_fraud
            from feast-oss.fraud_tutorial.transactions
            where
                timestamp > timestamp('{(last_timestamp - timedelta(days=2)).isoformat()}')"""
    elif datasource_type == DataSourceType.HIVE:

        last_timestamp = get_last_timestamp("transactions", "feature_timestamp",
                                            'hive://localhost:10000/fraud_tutorial')

        entity_df_selection_str = f"""
            select src_account as user_id, feature_timestamp, is_fraud
            from transactions
            where
                feature_timestamp >= unix_timestamp(\'{str(last_timestamp - timedelta(days=2))}\')
            """
    else:
        assert False

    # Getting training data from feature store...
    store = FeatureStore(
        repo_path=str(cur_dir_path.parent.parent / f"repos/fraud_{datasource_type.name.lower()}_repo"))

    training_data = store.get_historical_features(
        entity_df=entity_df_selection_str,
        features=[
            "user_transaction_count_7d:transaction_count_7d",
            "user_account_features:credit_score",
            "user_account_features:account_age_days",
            "user_account_features:user_has_2fa_installed",
            "user_has_fraudulent_transactions:user_has_fraudulent_transactions_7d"
        ],
        full_feature_names=True
    ).to_df()

    print("historical features dataframe size:", training_data.shape)

    # Drop stray nulls
    training_data.dropna(inplace=True)

    print("historical features dataframe size w/o NA:", training_data.shape)

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

    # Make a test prediction
    joblib.dump(model, "model.joblib")
    print("prediction: ")
    print(predict(model, store, [{"user_id": "v5zlw0"}]))


if __name__ == "__main__":
    process_fraud_tutorial(datasource_type=DataSourceType.BIGQUERY)
