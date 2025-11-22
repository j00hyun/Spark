import sys
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '3.0'

from pyspark.ml import Pipeline
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


# Schema for the input CSV file
tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def main(inputs, model_file):

    # Load the CSV file
    data = spark.read.csv(inputs, schema=tmax_schema)

    # Create yesterday_tmax feature with SQL JOIN
    add_yesterday = SQLTransformer(
        statement="""
                SELECT
                    today.*,
                    yesterday.tmax AS yesterday_tmax
                FROM __THIS__ AS today
                LEFT JOIN __THIS__ AS yesterday
                  ON date_sub(today.date, 1) = yesterday.date
                 AND today.station = yesterday.station
            """
    )

    fill_null = SQLTransformer(
        statement="""
                SELECT *,
                       COALESCE(yesterday_tmax, 0.0) AS yesterday_tmax_filled
                FROM __THIS__
            """
    )

    # Add a new column: day of year (1–365)
    # This helps the model understand seasonal temperature patterns.
    add_dayofyear = SQLTransformer(
        statement="""
            SELECT *,
                   dayofyear(date) AS day_of_year
            FROM __THIS__
        """
    )

    # Combine useful columns into one feature vector
    # ML models in Spark need a single "features" column.
    assembler = VectorAssembler(
        inputCols=['latitude', 'longitude', 'elevation', 'day_of_year', 'yesterday_tmax_filled'],
        outputCol='features'
    )

    # Use a Random Forest model
    # This is a non-linear model that works well for weather prediction.
    rf = RandomForestRegressor(
        featuresCol='features',
        labelCol='tmax',
        predictionCol='prediction',
        numTrees=50, # number of trees in the forest (more trees = better but slower)
        maxDepth=10  # how deep each tree can grow (higher = more complex model)
    )

    # Build the full ML pipeline
    pipeline = Pipeline(stages=[add_yesterday, fill_null, add_dayofyear, assembler, rf])

    # Split data into training (75%) and validation (25%)
    train, validation = data.randomSplit([0.75, 0.25])

    # Train the model
    model = pipeline.fit(train)

    # Make predictions on the validation set
    predictions = model.transform(validation)

    # Evaluate using R² and RMSE
    evaluator_r2 = RegressionEvaluator(
        labelCol='tmax',
        predictionCol='prediction',
        metricName='r2'
    )

    evaluator_rmse = RegressionEvaluator(
        labelCol='tmax',
        predictionCol='prediction',
        metricName='rmse'
    )

    r2 = evaluator_r2.evaluate(predictions)
    rmse = evaluator_rmse.evaluate(predictions)

    print("Validation R²:", r2)
    print("Validation RMSE:", rmse)

    # Save the trained model
    model.write().overwrite().save(model_file)
    print(f"Model saved to: {model_file}")

    print("Feature Importances:", model.stages[-1].featureImportances)

if __name__ == '__main__':
    inputs = sys.argv[1]         # e.g., tmax-1
    model_file = sys.argv[2]     # e.g., weather-model
    main(inputs, model_file)
