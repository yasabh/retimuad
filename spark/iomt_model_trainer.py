from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Path to your local dataset
LOCAL_FILE_PATH = "Datasets/IoMT_Dataset.csv"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IoMT_Local_Training") \
    .getOrCreate()

# Load Local File
# Adjust schema and options according to your file format
data = spark.read.csv(LOCAL_FILE_PATH, header=True, inferSchema=True)

# Assemble Features
assembler = VectorAssembler(
    inputCols=["SrcBytes", "DstBytes", "SrcLoad", "DstLoad", "Temp", "SpO2", "Pulse_Rate", "SYS", "DIA", "Heart_rate"],
    outputCol="features"
)
feature_df = assembler.transform(data)

# Train Model
lr = LinearRegression(featuresCol="features", labelCol="Label", maxIter=10, regParam=0.1, elasticNetParam=0.8)
model = lr.fit(feature_df)

# Save Model
model.save("models/iomt")
