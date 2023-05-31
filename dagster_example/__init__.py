"""Definitions that provide Dagster code locations."""
from dagster import Definitions, ScheduleDefinition

from dagster_example.assets.cereal import cereals, highest_calorie_cereal, highest_protein_cereal
from dagster_example.jobs import complex_job, hello_cereal_job, my_first_dbt_model
from dagster_example.schedules import every_weekday_9am

defs = Definitions(
    assets=[cereals, highest_calorie_cereal, highest_protein_cereal],
    jobs=[complex_job, hello_cereal_job, my_first_dbt_model],
    schedules=[every_weekday_9am, ScheduleDefinition(
        job=my_first_dbt_model, cron_schedule="@hourly")],
)
