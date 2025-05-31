#!/usr/bin/env python3

import pandas as pd
from pyspark.sql import SparkSession
import os


def main():
    # Step 1: Create a single parquet file using pandas

    # Create sample data
    data = {
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"],
        "age": [25, 30, 35, 28, 32, 29],
        "salary": [85000.0, 92000.0, 78000.0, 88000.0, 95000.0, 72000.0],
        "department": ["Engineering", "Engineering", "Marketing", "Sales", "Engineering", "Marketing"],
    }

    # Create pandas DataFrame
    df_pandas = pd.DataFrame(data)

    print("Sample data created with pandas:")
    print(df_pandas)
    print(f"DataFrame shape: {df_pandas.shape}")
    print(f"Data types:\n{df_pandas.dtypes}")

    # Define output path for single parquet file (in script directory)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parquet_file_path = os.path.join(script_dir, "employees_pandas.parquet")

    # Write to parquet using pandas (creates single file)
    df_pandas.to_parquet(parquet_file_path)

    print(f"\n✓ Parquet file created with pandas: {parquet_file_path}")
    print(f"File size: {os.path.getsize(parquet_file_path)} bytes")

    # Step 2: Create a local SparkSession
    spark = (
        SparkSession.builder.appName("ReadPandasParquet")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    print("\n✓ SparkSession created successfully")

    # Step 3: Read the pandas-created parquet file with PySpark

    # Read the single parquet file
    df_spark = spark.read.parquet(parquet_file_path)

    print("\n" + "=" * 50)
    print("DATA READ WITH PYSPARK:")
    print("=" * 50)

    # Show the data
    df_spark.show()

    # Show schema (notice PySpark inferred types from parquet metadata)
    print("Schema inferred by PySpark:")
    df_spark.printSchema()

    # Show some DataFrame info
    print(f"Row count: {df_spark.count()}")
    print(f"Column count: {len(df_spark.columns)}")
    print(f"Columns: {df_spark.columns}")

    # Demonstrate some PySpark operations
    print("\n" + "=" * 30)
    print("PYSPARK OPERATIONS:")
    print("=" * 30)

    # Group by department
    print("Employee count by department:")
    df_spark.groupBy("department").count().orderBy("department").show()

    # Average salary by department
    print("Average salary by department:")
    df_spark.groupBy("department").avg("salary").withColumnRenamed("avg(salary)", "avg_salary").orderBy(
        "department"
    ).show()

    # Filter high earners
    print("Employees earning more than $80,000:")
    df_spark.filter(df_spark.salary > 80000).select("name", "salary", "department").orderBy(
        "salary", ascending=False
    ).show()

    # Create a temporary view for SQL queries
    df_spark.createOrReplaceTempView("employees")

    print("SQL query example:")
    result = spark.sql(
        """
                       SELECT department,
                              COUNT(*) as employee_count,
                              ROUND(AVG(salary), 2) as avg_salary,
                              MAX(age) as oldest_employee_age
                       FROM employees
                       GROUP BY department
                       ORDER BY avg_salary DESC
                       """
    )
    result.show()

    # Clean up
    spark.stop()
    print("\n✓ SparkSession stopped")

    # Optionally remove the parquet file
    # os.remove(parquet_file_path)
    print(f"Parquet file saved in script directory: {parquet_file_path}")


if __name__ == "__main__":
    main()
