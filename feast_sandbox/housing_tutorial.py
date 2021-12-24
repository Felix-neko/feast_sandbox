from pathlib import Path
from datetime import datetime, timedelta

import feast
import pandas as pd

from feast_sandbox.utils import RepoType

cur_dir_path = Path(__file__).absolute().parent

if __name__ == "__main__":
    # print(pd.DataFrame.from_dict([{"id": 1, "hell": "star"}]).head())

    timestamp1 = datetime.fromisoformat("2015-11-08")
    timestamp2 = datetime.fromisoformat("2018-01-01")

    entity_df = pd.DataFrame.from_dict(
        [
            {"house_id": 1, "buyer_id": 2, "event_timestamp": timestamp1},
            {"house_id": 2, "buyer_id": 2, "event_timestamp": timestamp2}
        ]
    )

    features_to_extract = [
        "house:age",
        "house:name",
        "buyer:age",
        "buyer:name",
        "buyer:yearly_income",
        "mortgage:name",
        "mortgage:value",
        "mortgage:due_date"
        ]

    store = feast.FeatureStore(str(cur_dir_path.parent / "repos/housing_hive_repo"))

    hist_features = store.get_historical_features(entity_df=entity_df, features=features_to_extract,
                                                  full_feature_names=True)

    print(hist_features)
    df = hist_features.to_df()
    print(df)  # Hmm, why does it give me NaNs???