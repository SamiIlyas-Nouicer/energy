import mlflow
from mlflow import sklearn as mlflow_sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import train_test_split
import pandas as pd
# import duckdb # Assuming your gold layer is in DuckDB based on your folder structure

# 1. Set the Tracking URI to your Docker container
mlflow.set_tracking_uri("http://localhost:5000")

# 2. Name your experiment
# Change this line:
mlflow.set_experiment("Energy_Consumption_Forecast_v4")
def load_gold_data():
    # TODO: Load your gold data here! 
    # For example, querying your gold.duckdb file to get a pandas DataFrame
    # df = duckdb.connect('../data/gold.duckdb').execute("SELECT * FROM gold_energy_weather").df()
    
    # Dummy data for testing the MLflow connection:
    data = {
        'temperature': [15, 20, 25, 10, 5],
        'wind_speed': [10, 15, 5, 20, 25],
        'consumption': [50000, 52000, 55000, 60000, 65000]
    }
    return pd.DataFrame(data)

def train():
    df = load_gold_data()
    X = df[['temperature', 'wind_speed']]
    y = df['consumption']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 3. Start the MLflow Run
    with mlflow.start_run(run_name="RandomForest_Baseline"):
        
        # Define parameters
        n_estimators = 100
        max_depth = 5
        
        # Log parameters to MLflow
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("max_depth", max_depth)
        
        # Train model
        model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth)
        model.fit(X_train, y_train)
        
        # Evaluate
        # Evaluate
        predictions = model.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        rmse = mse ** 0.5  # Calculate the square root manually
        mae = mean_absolute_error(y_test, predictions)
        # Log metrics to MLflow
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        
        # Log the actual model artifact
        mlflow_sklearn.log_model(model, "rf_model")
        
        print(f"✅ Model trained! RMSE: {rmse:.2f}, MAE: {mae:.2f}")

if __name__ == "__main__":
    train()