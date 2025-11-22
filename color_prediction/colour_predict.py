import sys
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '3.0'

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):

    # ---------------------------------------------------------
    # 1. LOAD DATA
    # ---------------------------------------------------------
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # ---------------------------------------------------------
    # 2. RGB PIPELINE
    # ---------------------------------------------------------

    # (1) VectorAssembler: R,G,B → features
    rgb_assembler = VectorAssembler(
        inputCols=['R', 'G', 'B'],
        outputCol='features'
    )

    # (2) StringIndexer: word → label
    word_indexer = StringIndexer(
        inputCol='word',
        outputCol='label'
    )

    # (3) Classifier (MultilayerPerceptron)
    classifier = MultilayerPerceptronClassifier(
        layers=[3, 30, 11],   # 3 input, 30 hidden, 11 output classes
        featuresCol='features',
        labelCol='label',
        predictionCol='prediction'
    )

    # (4) RGB Pipeline
    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)

    # (5) Evaluate RGB model
    evaluator = MulticlassClassificationEvaluator(
        labelCol='label',
        predictionCol='prediction',
        metricName='accuracy'
    )

    rgb_predictions = rgb_model.transform(validation)
    rgb_score = evaluator.evaluate(rgb_predictions)

    # (6) Save RGB prediction plot
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model:', rgb_score)

    # ---------------------------------------------------------
    # 3. LAB PIPELINE
    # ---------------------------------------------------------

    # Create SQL query to convert RGB → LAB
    rgb_to_lab = SQLTransformer(
        statement=rgb2lab_query(passthrough_columns=['word'])
    )

    # LAB assembler: labL, labA, labB → features
    lab_assembler = VectorAssembler(
        inputCols=['labL', 'labA', 'labB'],
        outputCol='features'
    )

    # LAB pipeline
    lab_pipeline = Pipeline(stages=[rgb_to_lab, lab_assembler, word_indexer, classifier])
    lab_model = lab_pipeline.fit(train)

    # Evaluate LAB model
    lab_predictions = lab_model.transform(validation)
    lab_score = evaluator.evaluate(lab_predictions)

    # Save LAB prediction plot
    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('Validation score for LAB model:', lab_score)

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
