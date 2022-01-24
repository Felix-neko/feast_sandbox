
from pathlib import Path

from feast.repo_operations import (
    cli_check_repo,
    apply_total
)

from feast.repo_config import load_repo_config
from feast import FeatureStore


if __name__ == "__main__":

    # Please execute `create_schemas.sql` with Hive before start!

    # Here we have two Hive bases (`housing` and `housing2`) that both have a table named `house`.
    # Vanilla `feast-hive` will crash on it because of problems with table name resolution.

    # It's an analogue of `feast apply` command for `/repos/housing/hive_local_table_search_bug` feast repo.

    repo_path = Path(__file__).absolute().parent.parent.parent / "repos/housing/hive_local_table_search_bug"
    store = FeatureStore(repo_path)

    cli_check_repo(repo_path)
    repo_config = load_repo_config(repo_path)
    apply_total(repo_config, repo_path, False)  # Should crash here



    # Don't forget to execute `drop_schemas.sql` to clean up