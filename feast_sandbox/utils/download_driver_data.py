"""
Let's download driver statistics data from FEAST driver ranking tutorial and save it to driver_data.parquet
(GCP CREDENTIALS env var needed!)
"""

from google.cloud import bigquery
if __name__ == "__main__":

    bqclient = bigquery.Client()

    # Download query results.
    query_string = """
    SELECT * FROM feast-oss.demo_data.driver_hourly_stats
    """

    dataframe = (
        bqclient.query(query_string)
            .result()
            .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    print(dataframe.head())
    with open("driver_data.parquet", "wb") as out_file:
        out_file.write(dataframe.to_parquet())
