from pprint import pprint
from feast import FeatureStore
import pandas as pd
from datetime import datetime, timedelta

if __name__ == "__main__":
    # The entity dataframe is the dataframe we want to enrich with feature values

    fs = FeatureStore("../feature_repo")

    entity_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001, 1002, 1003],
            "event_timestamp": [
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(days=2),
            ],
        }
    )

    store = FeatureStore(repo_path="../feature_repo")

    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
        ],
    ).to_df()

    print("----- Feature schema -----\n")
    print(training_df.info())

    print()
    print("----- Example features -----\n")
    print(training_df.head())

    pass

