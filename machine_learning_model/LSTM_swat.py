import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
import os
import pickle

# Set seeds for reproducibility
np.random.seed(13)
tf.random.set_seed(13)

# Load and preprocess data
def load_and_preprocess_data(file_path):
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip()
    # Selecting relevant features based on the SWaT dataset
    features_considered = ['FIT101', 'LIT101', 'AIT201', 'AIT202', 'AIT203']
    df = df[features_considered]
    # df['MV101'] = df['MV101'].astype('int')  # Assuming MV101 and P101 are binary
    # df['P101'] = df['P101'].astype('int')
    scaler = MinMaxScaler()
    df[['FIT101', 'LIT101', 'AIT201', 'AIT202', 'AIT203']] = scaler.fit_transform(df[['FIT101', 'LIT101', 'AIT201', 'AIT202', 'AIT203']])
    return df.values, scaler

# Get script folder paths for models and scalers
script_folder = os.path.dirname(os.path.abspath(__file__))
model_folder = os.path.join(script_folder, 'models')
scaler_folder = os.path.join(script_folder, 'scalers')

# Create necessary directories
os.makedirs(model_folder, exist_ok=True)
os.makedirs(scaler_folder, exist_ok=True)

# Prepare the dataset path and load data
folder_path = os.path.join(script_folder, '../dataset')
data_path = os.path.join(folder_path, 'preprocess_swat.csv')
dataset, scaler = load_and_preprocess_data(data_path)

# Standardize data
TRAIN_SPLIT = 8097
data_mean = dataset[:TRAIN_SPLIT].mean(axis=0)
data_std = dataset[:TRAIN_SPLIT].std(axis=0)
dataset = (dataset - data_mean) / data_std

# Helper function to create batches of data
def multivariate_data(dataset, target, start_index, end_index, history_size, target_size, step, single_step=False):
    data = []
    labels = []

    start_index = start_index + history_size
    if end_index is None:
        end_index = len(dataset) - target_size

    for i in range(start_index, end_index):
        indices = range(i-history_size, i, step)
        data.append(dataset[indices])

        if single_step:
            labels.append(target[i + target_size])
        else:
            labels.append(target[i : i + target_size])

    return np.array(data), np.array(labels)

# Model configuration
past_history = 720
future_target = 72
STEP = 1

x_train, y_train = multivariate_data(dataset, dataset[:, 1], 0, TRAIN_SPLIT, past_history, future_target, STEP)
x_val, y_val = multivariate_data(dataset, dataset[:, 1], TRAIN_SPLIT, None, past_history, future_target, STEP)

# Create TensorFlow datasets
train_data = tf.data.Dataset.from_tensor_slices((x_train, y_train))
train_data = train_data.cache().shuffle(10000).batch(256).repeat()
val_data = tf.data.Dataset.from_tensor_slices((x_val, y_val))
val_data = val_data.batch(256).repeat()

# # Define the standard LSTM model
def create_model():
    model = tf.keras.models.Sequential([
        tf.keras.layers.LSTM(64, input_shape=x_train.shape[-2:]),
        tf.keras.layers.Dense(units=future_target)
    ])
    model.compile(optimizer='adam', loss='mean_squared_error')
    return model

# # Define the complex LSTM model
# def create_model():
#     model = tf.keras.models.Sequential([
#         tf.keras.layers.LSTM(64, return_sequences=True, input_shape=x_train.shape[-2:]),
#         tf.keras.layers.LSTM(32),
#         tf.keras.layers.Dense(50, activation='relu'),
#         tf.keras.layers.Dense(units=future_target)
#     ])
#     model.compile(optimizer='adam', loss='mean_squared_error')
#     return model

model = create_model()
model.summary()

# Train the model
history = model.fit(train_data, epochs=10, steps_per_epoch=200, validation_data=val_data, validation_steps=50)

# Save the model in the designated model folder
model_path = os.path.join(model_folder, 'swat_lstm_model.h5')
model.save(model_path)
print(f'Model saved to {model_path}')

# Save the scaler for future use
scaler_path = os.path.join(scaler_folder, 'swat_scaler.pkl')
with open(scaler_path, 'wb') as f:
    pickle.dump(scaler, f)
print(f'Scaler saved to {scaler_path}')

# Plot training history
plt.plot(history.history['loss'], label='Training loss')
plt.plot(history.history['val_loss'], label='Validation loss')
plt.legend()
plt.show()

# Visualization of model predictions against actual data
def multi_step_plot(history, true_future, prediction):
    plt.figure(figsize=(12, 6))
    num_in = list(range(-len(history), 0))
    num_out = len(true_future)

    plt.plot(num_in, np.array(history[:, 1]), label='History')
    plt.plot(np.arange(num_out), np.array(true_future), 'bo', label='True Future')
    if prediction.any():
        plt.plot(np.arange(num_out), np.array(prediction), 'ro', label='Predicted Future')
    plt.legend(loc='upper left')
    plt.show()

for x, y in val_data.take(3):
    multi_step_plot(x[0].numpy(), y[0].numpy(), model.predict(x)[0])