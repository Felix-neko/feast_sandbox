# This is an example feature definition file

from datetime import timedelta

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    Project,
    PushSource,
    RequestSource,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64

# Define a project for the feature repo
project = Project(name="purchases", description="A project for driver statistics")

client_entity = Entity(name="client_entity", join_keys=["client_id"])

client_source = FileSource(name="client_source", path="data/clients.parquet", timestamp_field="signup_timestamp")
client_view = FeatureView(name="client_view", entities=[client_entity], source=client_source)

purchase_entity = Entity(name="purchase_entity", join_keys=["purchase_id"])
purchase_source = FileSource(
    name="purchase_source", path="data/purchases.parquet", timestamp_field="purchase_timestamp"
)
purchase_view = FeatureView(name="purchase_view", entities=[purchase_entity], source=purchase_source)
