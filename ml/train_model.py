import mlflow
import mlflow.sklearn
import pandas as pd
from google.cloud import bigquery
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from sklearn.preprocessing import LabelEncoder
import numpy as np
import os

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
PROJECT_ID = "healthcare-pipeline-489402"

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("healthcare-revenue-prediction")

client = bigquery.Client(project=PROJECT_ID)

query = """
    SELECT
        total_services,
        total_beneficiaries,
        avg_submitted_charge,
        avg_medicare_payment
    FROM `healthcare-pipeline-489402.healthcare_analytics.fact_claims`
    WHERE avg_medicare_payment > 0
"""

print("Fetching data from BigQuery...")
df = client.query(query).result().to_dataframe(create_bqstorage_client=False)

print(f"Rows fetched: {len(df)}")



X = df.drop("avg_medicare_payment", axis=1)
y = df["avg_medicare_payment"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

models = {
    "linear_regression": LinearRegression(),
    "random_forest": RandomForestRegressor(n_estimators=100, max_depth=5, random_state=42),
    "gradient_boosting": GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)
}

for model_name, model in models.items():
    with mlflow.start_run(run_name=model_name):
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        r2 = r2_score(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae = mean_absolute_error(y_test, y_pred)

        mlflow.log_param("model", model_name)
        mlflow.log_param("test_size", 0.2)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.sklearn.log_model(model, artifact_path=model_name)

        print(f"{model_name} -> R2: {r2:.4f}, RMSE: {rmse:.4f}, MAE: {mae:.4f}")

print("Training complete. Check MLflow UI.")
