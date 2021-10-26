from feast_sandbox.utils import download_bigquery_dataframe

if __name__ == "__main__":
    download_bigquery_dataframe("feast-oss.fraud_tutorial.transactions", "transactions.parquet")
    download_bigquery_dataframe("feast-oss.fraud_tutorial.user_has_fraudulent_transactions",
                                "user_has_fraudulent_transactions.parquet")
    download_bigquery_dataframe("feast-oss.fraud_tutorial.user_account_features", "user_account_features.parquet")