"""Collection of Cereal jobs"""
from dagster import file_relative_path, job
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_example.ops.cereal import (
    display_results,
    download_cereals,
    find_highest_calorie_cereal,
    find_highest_protein_cereal,
    hello_cereal,
)

DBT_PROJECT_PATH = file_relative_path(__file__, '../example')
DBT_PROFILES = file_relative_path(__file__, '../example')

dbt_resource = dbt_cli_resource.configured({
    "project_dir": DBT_PROJECT_PATH, "profiles_dir": DBT_PROFILES, "models": ["my_first_dbt_model"]
})


@job(resource_defs={"dbt": dbt_resource})
def my_first_dbt_model():
    dbt_run_op()


@job
def hello_cereal_job():
    """Example of a simple Dagster job."""
    hello_cereal()


@job
def complex_job():
    """Example of a more complex Dagster job."""
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_protein=find_highest_protein_cereal(cereals),
    )
