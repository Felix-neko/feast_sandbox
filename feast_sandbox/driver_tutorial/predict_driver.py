from pathlib import Path
from datetime import datetime
from joblib import dump, load


import pandas as pd
import feast
from joblib import load
from sklearn.linear_model import LinearRegression

from feast_sandbox.utils import RepoType

cur_dir_path = Path(__file__).absolute().parent


def train_ranking_model(repo_type: RepoType = RepoType.HIVE):
    # Load driver order data

    assert repo_type in [RepoType.HIVE, RepoType.PARQUET]

    cur_dir_path = Path(__file__).absolute().parent
    orders = pd.read_csv("driver_orders.csv", sep="\t")
    orders["event_timestamp"] = pd.to_datetime(orders["event_timestamp"])

    # Connect to your local feature store
    fs = feast.FeatureStore(str(cur_dir_path.parent.parent / f"repos/driver_{repo_type.name.lower()}_repo"))

    # Retrieve training data from BigQuery
    training_df = fs.get_historical_features(
        entity_df=orders,
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
        ],
    ).to_df()

    # Train model
    target = "trip_completed"

    reg = LinearRegression()
    print(training_df.columns)
    train_X = training_df[training_df.columns.drop(target).drop("event_timestamp")]
    train_Y = training_df.loc[:, target]
    reg.fit(train_X[sorted(train_X)], train_Y)

    # Save model
    dump(reg, "../driver_model.bin")


class DriverRankingModel:
    def __init__(self, repo_type: RepoType = RepoType.HIVE):
        # Load model
        self.model = load("../driver_model.bin")

        # Set up feature store
        self.fs = feast.FeatureStore(str(cur_dir_path.parent.parent / f"repos/driver_{repo_type.name.lower()}_repo"))

        # self.fs.materialize_incremental(datetime.now())

    def predict(self, driver_ids):
        # Read features from Feast
        driver_features = self.fs.get_online_features(
            entity_rows=[{"driver_id": driver_id} for driver_id in driver_ids],
            features=[
                "driver_hourly_stats:conv_rate",
                "driver_hourly_stats:acc_rate",
                "driver_hourly_stats:avg_daily_trips",
            ],
        )
        df = pd.DataFrame.from_dict(driver_features.to_dict())

        # Make prediction
        df["prediction"] = self.model.predict(df[sorted(df)])

        # Choose best driver
        best_driver_id = df["driver_id"].iloc[df["prediction"].argmax()]

        # return best driver
        return best_driver_id


def predict_best_driver(repo_type: RepoType = RepoType.HIVE):
    drivers = [1001, 1002, 1003, 1004]
    model = DriverRankingModel(repo_type)
    best_driver = model.predict(drivers)
    print(f"best driver: {best_driver}")


if __name__ == "__main__":
    repo_type = RepoType.HIVE
    train_ranking_model(repo_type)
    predict_best_driver(repo_type)
