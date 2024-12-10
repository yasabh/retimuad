import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Conv1D, Dense, Flatten
from tensorflow.keras.optimizers import Adam
import joblib

# Load and preprocess data
def load_data(file_path):
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip()
    selected_columns = ['FIT101', 'LIT101', 'MV101', 'P101']  # Modify as necessary
    df = df[selected_columns]
    return df

def normalize_data(df):
    scaler = MinMaxScaler()
    normalized_data = scaler.fit_transform(df)
    return normalized_data, scaler

# Build 1D-CNN model
def build_model(input_shape):
    model = Sequential([
        Conv1D(filters=64, kernel_size=3, activation='relu', input_shape=(input_shape, 1)),
        Flatten(),
        Dense(50, activation='relu'),
        Dense(1)
    ])
    model.compile(optimizer=Adam(), loss='mse')
    return model

# CUSUM algorithm for deviation detection
def cusum_algorithm(values, threshold=5):
    cumulative_sum = 0
    alarms = []
    for value in values:
        cumulative_sum += value
        if abs(cumulative_sum) > threshold:
            alarms.append(True)
            cumulative_sum = 0  # reset after an alarm
        else:
            alarms.append(False)
    return alarms

def main():
    script_folder = os.path.dirname(os.path.abspath(__file__))
    model_folder = os.path.join(script_folder, 'models')
    scaler_folder = os.path.join(script_folder, 'scalers')

    os.makedirs(model_folder, exist_ok=True)
    os.makedirs(scaler_folder, exist_ok=True)

    folder_path = os.path.join(script_folder, '../dataset')
    data_path = os.path.join(folder_path, 'preprocess_swat.csv')
    df = load_data(data_path)
    normalized_data, scaler = normalize_data(df)
    
    train_size = int(len(normalized_data) * 0.7)
    train, test = normalized_data[:train_size], normalized_data[train_size:]
    
    train = train.reshape((train.shape[0], train.shape[1], 1))
    test = test.reshape((test.shape[0], test.shape[1], 1))

    model = build_model(train.shape[1])
    model.fit(train, train[:, :, 0], epochs=10, batch_size=32)  # Assuming the first feature is what we're predicting
    
    model.save(os.path.join(model_folder, '1d_cnn_model.keras'))
    joblib.dump(scaler, os.path.join(scaler_folder, 'data_scaler.pkl'))

    # Assuming we're predicting the first feature, adjust actuals accordingly
    predictions = model.predict(test).squeeze()
    actuals = test[:, 0, 0]  # Only select the first feature from the actuals for comparison

    # Debug output to confirm shapes
    print(f'Predictions shape: {predictions.shape}')
    print(f'Actuals shape: {actuals.shape}')

    mae = mean_absolute_error(actuals, predictions)
    rmse = np.sqrt(mean_squared_error(actuals, predictions))
    print("Mean Absolute Error:", mae)
    print("Root Mean Square Error:", rmse)

if __name__ == '__main__':
    main()
