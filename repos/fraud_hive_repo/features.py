from datetime import timedelta

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, FileSource, ValueType
from feast_hive import HiveSource

# Add an entity for users
user_entity = Entity(
    name="user_id",
    description="A user that has executed a transaction or received a transaction",
    value_type=ValueType.STRING
)

user_transactions_count_source = HiveSource(
    table="user_count_transactions_7d", event_timestamp_column="feature_timestamp"
)

# Add a FeatureView based on our new table
user_transactions_count_fv = FeatureView(
    name="user_transaction_count_7d",
    entities=["user_id"],
    ttl=timedelta(weeks=1),
    batch_source=user_transactions_count_source)

user_account_source = HiveSource(table="user_account_features", event_timestamp_column="feature_timestamp")

# Add two FeatureViews based on existing tables in BigQuery
user_account_fv = FeatureView(
    name="user_account_features",
    entities=["user_id"],
    ttl=timedelta(weeks=52),
    batch_source=user_account_source)

user_has_fraudulent_transactions_source = HiveSource(table="user_has_fraudulent_transactions",
                                                     event_timestamp_column="feature_timestamp")

user_has_fraudulent_transactions_fv = FeatureView(
    name="user_has_fraudulent_transactions",
    entities=["user_id"],
    ttl=timedelta(weeks=52),
    batch_source=user_has_fraudulent_transactions_source)