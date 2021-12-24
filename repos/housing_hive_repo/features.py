from datetime import timedelta

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, FileSource, ValueType
from feast_hive import HiveSource

FEATURE_TTL = timedelta(days=365 * 40)

buyer_entity = Entity(name="buyer_id", description="TODO: BUYER")
buyer_source = HiveSource(table="buyer", event_timestamp_column="event_timestamp")
buyer_fv = FeatureView(name="buyer", entities=["buyer_id"], ttl=FEATURE_TTL, batch_source=buyer_source)

house_entity = Entity(name="house_id", description="TODO: HOUSE")
house_source = HiveSource(table="house", event_timestamp_column="event_timestamp")
house_fv = FeatureView(name="house", entities=["house_id"], ttl=FEATURE_TTL, batch_source=house_source)

# mortgage_entity = Entity(name="mortgage_id", description="TODO: MORTGAGE")
mortgage_source = HiveSource(table="mortgage", event_timestamp_column="event_timestamp")
mortgage_fv = FeatureView(name="mortgage", entities=["buyer_id", "house_id"],
                          ttl=FEATURE_TTL, batch_source=mortgage_source)



