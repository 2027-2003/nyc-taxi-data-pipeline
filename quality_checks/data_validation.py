import great_expectations as gx
import pandas as pd
import os

def validate_with_gx():
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base_path, "data_lake", "curated", "transformed_data.parquet")

    print("loading data for validation...")
    df = pd.read_parquet(path)

    context = gx.get_context()

    data_source = context.data_sources.add_pandas("taxi_datasource")
    data_asset = data_source.add_dataframe_asset("taxi_trips")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(gx.ExpectationSuite(name="taxi_suite"))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="total_amount"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="trip_distance"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="pickup_hour", min_value=0, max_value=23))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1, max_value=8))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0, max_value=1200))

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name="taxi_validation", data=batch_definition, suite=suite)
    )

    results = validation_definition.run(batch_parameters={"dataframe": df})

    print(f"passed: {results.statistics['successful_expectations']}")
    print(f"failed: {results.statistics['unsuccessful_expectations']}")

    for result in results.results:
      if not result.success:
        print(f"Failed: {result.expectation_config}")

if __name__ == "__main__":
    validate_with_gx()
