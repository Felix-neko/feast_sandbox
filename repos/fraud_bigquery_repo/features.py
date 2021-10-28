from datetime import timedelta
from feast import BigQuerySource, FeatureView, Entity, ValueType

# Add an entity for users
user_entity = Entity(
    name="user_id",
    description="A user that has executed a transaction or received a transaction",
    value_type=ValueType.STRING
)

# Add a FeatureView based on our new table
driver_stats_fv = FeatureView(
    name="user_transaction_count_7d",
    entities=["user_id"],
    ttl=timedelta(weeks=1),
    batch_source=BigQuerySource(
        table_ref=f"integral-server-329913.feast_fraud_ds.user_count_transactions_7d",
        event_timestamp_column="feature_timestamp"))

# Add two FeatureViews based on existing tables in BigQuery
user_account_fv = FeatureView(
    name="user_account_features",
    entities=["user_id"],
    ttl=timedelta(weeks=52),
    batch_source=BigQuerySource(
        table_ref=f"feast-oss.fraud_tutorial.user_account_features",
        event_timestamp_column="feature_timestamp"))

user_has_fraudulent_transactions_fv = FeatureView(
    name="user_has_fraudulent_transactions",
    entities=["user_id"],
    ttl=timedelta(weeks=52),
    batch_source=BigQuerySource(
        table_ref=f"feast-oss.fraud_tutorial.user_has_fraudulent_transactions",
        event_timestamp_column="feature_timestamp"))