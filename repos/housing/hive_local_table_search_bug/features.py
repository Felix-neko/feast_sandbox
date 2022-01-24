from datetime import timedelta

from feast import Entity, FeatureView
from feast_hive import HiveSource

FEATURE_TTL = timedelta(days=365 * 40)

house_entity = Entity(name="house_id", description="TODO: HOUSE")
house_source = HiveSource(table="house", event_timestamp_column="event_timestamp")
house_fv = FeatureView(name="house", entities=["house_id"], ttl=FEATURE_TTL, batch_source=house_source)
