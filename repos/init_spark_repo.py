from feast.cli.cli import init_repo

init_repo(
    template="spark",
    repo_name="spark_tutorial_2",
)


import pyarrow as pa
import pyarrow.parquet as pq

# Path to your Parquet file
parquet_path = "/home/felix/Projects/feast_sandbox/repos/spark_tutorial_2/feature_repo/data/driver_hourly_stats.parquet"

# Open the Parquet file with PyArrow
pf = pq.ParquetFile(parquet_path)

# Retrieve the Arrow schema (which knows about timestamp units)
arrow_schema = pf.schema_arrow

# Iterate over all fields in the schema
for field in arrow_schema:
    # Check if the field’s Arrow type is a timestamp
    if pa.types.is_timestamp(field.type):
        # Arrow stores timestamps with a ".unit" attribute
        # field.type.unit will be one of: "s", "ms", "us", or "ns"
        unit = field.type.unit
        print(f"Column '{field.name}' is a timestamp (unit = {unit}).")
