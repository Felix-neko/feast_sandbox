import subprocess
from datetime import datetime

import pandas as pd

from feast import FeatureStore


def main():
    print("\n--- Run feast apply ---")
    subprocess.run(["feast", "apply"])

    store = FeatureStore(repo_path=".")

    purchase_entity = store.get_entity("purchase_entity")

    # And how can I list all purchases if I don't know their distinct IDs?


if __name__ == "__main__":
    main()
