import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import create_map, lit, col
from pyspark.ml.feature import VectorAssembler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.utils import to_categorical
from sklearn.model_selection import train_test_split

# Configurations
DATASET_FILE = "Datasets/IoMT_Dataset.csv"
MODEL_PATH = "iomt"
CATEGORY_MAPPING = {
    "normal": 0,
    "Spoofing": 1,
    "Data Alteration": 2
}

# Initialize Spark Session
spark = SparkSession.builder.appName("IoMT_Local_Training").getOrCreate()

# Load Dataset
data = spark.read.csv(DATASET_FILE, header=True, inferSchema=True)

# Map 'Attack Category' to Numeric Labels
mapping_expr = create_map([lit(x) for item in CATEGORY_MAPPING.items() for x in item])
data = data.withColumn("Label", mapping_expr[col("Attack Category")]).drop("Attack Category")

# Assemble Features
selected_features = [
    "SrcBytes", "DstBytes", "SrcLoad", "DstLoad", "Temp", "SpO2", "Pulse_Rate",
    "SYS", "DIA", "Heart_rate", "Dur", "TotBytes", "TotPkts", "Rate", "pLoss",
    "pSrcLoss", "pDstLoss", "SrcJitter", "DstJitter", "sMaxPktSz", "dMaxPktSz",
    "sMinPktSz", "dMinPktSz", "SrcGap", "DstGap"
]
assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
feature_df = assembler.transform(data)

# Convert Data to NumPy Arrays
feature_array = np.array(feature_df.select("features").rdd.map(lambda row: row[0]).collect())
label_array = np.array(feature_df.select("Label").rdd.flatMap(lambda x: x).collect())

# One-Hot Encode Labels for Multi-Class Classification
label_array = to_categorical(label_array, num_classes=len(CATEGORY_MAPPING))

# Split Data for Training and Testing
X_train, X_test, y_train, y_test = train_test_split(feature_array, label_array, test_size=0.2, random_state=42)

# Define Neural Network Model
model = Sequential([
    Dense(128, activation='relu', input_shape=(X_train.shape[1],)),
    Dropout(0.3),
    Dense(64, activation='relu'),
    Dropout(0.2),
    Dense(32, activation='relu'),
    Dropout(0.1),
    Dense(len(CATEGORY_MAPPING), activation='softmax')  # Output layer for 3 classes
])

# Compile the Model
model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

# Early Stopping Callback
early_stopping = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)

# Train the Model
history = model.fit(
    X_train, y_train,
    epochs=50,
    batch_size=32,
    validation_data=(X_test, y_test),
    callbacks=[early_stopping]
)

# Save the Model
model.export(f"models/{MODEL_PATH}")

# Evaluate the Model
loss, accuracy = model.evaluate(X_test, y_test)
print(f"Test Loss: {loss:.4f}, Test Accuracy: {accuracy:.4f}")
