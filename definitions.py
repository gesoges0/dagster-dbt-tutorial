import dagster as dg
from dagster_dbt import dbt_assets, DbtCliResource, DbtProject
from pathlib import Path
import pandas as pd
import duckdb
import os

duckdb_database_path = "./dev.duckdb"


@dg.asset(compute_kind="python")
def raw_customers(context: dg.AssetExecutionContext) -> None:

    # Pull customer data from a CSV
    data = pd.read_csv("https://docs.dagster.io/assets/customers.csv")
    connection = duckdb.connect(os.fspath(duckdb_database_path))

    # Create a schema named raw
    connection.execute("create schema if not exists raw")

    # Create/replace table named raw_customers
    connection.execute(
        "create or replace table raw.raw_customers as select * from data"
    )

    # Log some metadata about the new table. It will show up in the UI.
    context.add_output_metadata({"num_rows": data.shape[0]})

# Points to the dbt project path
dbt_project_directory = Path(__file__).absolute().parent
dbt_project = DbtProject(project_dir=dbt_project_directory)

# References the dbt project object
dbt_resource = DbtCliResource(project_dir=dbt_project)

# Compiles the dbt project & allow Dagster to build an asset graph
dbt_project.prepare_if_dev()


# Yields Dagster events streamed from the dbt CLI
@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# Dagster object that contains the dbt assets and resource
defs = dg.Definitions(
    assets=[raw_customers, dbt_models],
    resources={"dbt": dbt_resource}
)
