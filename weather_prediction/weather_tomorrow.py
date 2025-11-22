import sys
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, Row, functions, types
from pyspark.ml import PipelineModel
from datetime import datetime

spark = SparkSession.builder.appName('weather tomorrow prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '3.0'

input_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType())   # yesterday's tmax goes here
])

def main(model_file):

    model = PipelineModel.load(model_file)

    latitude = 49.2771
    longitude = -122.9146
    elevation = 330.0

    yesterday_tmax = 12.0
    target_date = datetime.strptime("2025-11-22", "%Y-%m-%d").date()

    row = Row(
        station="SFU",
        date=target_date,
        latitude=latitude,
        longitude=longitude,
        elevation=elevation,
        tmax=yesterday_tmax     # yesterday value goes here
    )

    data = spark.createDataFrame([row], schema=input_schema)

    predictions = model.transform(data)
    pred_value = predictions.select('prediction').first()['prediction']

    print("Predicted tmax tomorrow:", pred_value)


if __name__ == '__main__':
    model_file = sys.argv[1]
    main(model_file)
