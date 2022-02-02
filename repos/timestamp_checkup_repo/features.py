from datetime import timedelta

from feast import Entity, FeatureView
from feast_hive import HiveSource

FEATURE_TTL = timedelta(days=365 * 40)

daemon = Entity(name="daemon", description="TODO: BUYER", join_key="name")
daemon_source = HiveSource(table="daemon", event_timestamp_column="event_timestamp",
                          created_timestamp_column="created_timestamp")
daemon_fv = FeatureView(name="daemon", entities=["daemon"], ttl=FEATURE_TTL, batch_source=daemon_source)
