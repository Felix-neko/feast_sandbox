from pathlib import Path

from feast_sandbox.utils import download_bigquery_dataframe


if __name__ == "__main__":

    cur_dir_path = Path(__file__).parent
    fraud_parquet_repo_path = cur_dir_path.parent.parent / "repos/fraud_parquet_repo"
    download_bigquery_dataframe("feast-oss.fraud_tutorial.transactions",
                                str(fraud_parquet_repo_path / "transactions.parquet"),
                                field_name_map={"timestamp": "feature_timestamp"})
    download_bigquery_dataframe("feast-oss.fraud_tutorial.user_has_fraudulent_transactions",
                                str(fraud_parquet_repo_path / "user_has_fraudulent_transactions.parquet"))
    download_bigquery_dataframe("feast-oss.fraud_tutorial.user_account_features",
                                str(fraud_parquet_repo_path / "user_account_features.parquet"))

    download_bigquery_dataframe("integral-server-329913.feast_fraud_ds.user_count_transactions_7d",
                                str(fraud_parquet_repo_path / "user_count_transactions_7d.parquet"))
