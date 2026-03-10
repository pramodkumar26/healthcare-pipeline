import os
from flask import Flask, render_template, jsonify
from google.cloud import bigquery

app = Flask(__name__)

PROJECT_ID = "healthcare-pipeline-489402"
DATASET = "healthcare_analytics"

def run_query(query):
    client = bigquery.Client(project=PROJECT_ID)
    return client.query(query).result().to_dataframe(create_bqstorage_client=False)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/revenue-by-provider")
def revenue_by_provider():
    query = """
        SELECT
            p.provider_type,
            ROUND(SUM(f.total_medicare_payment), 2) as total_payment
        FROM `{}.{}.fact_claims` f
        JOIN `{}.{}.dim_provider` p ON f.provider_id = p.provider_id
        GROUP BY p.provider_type
        ORDER BY total_payment DESC
        LIMIT 10
    """.format(PROJECT_ID, DATASET, PROJECT_ID, DATASET)
    df = run_query(query)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/revenue-by-state")
def revenue_by_state():
    query = """
        SELECT
            p.state,
            ROUND(SUM(f.total_medicare_payment), 2) as total_payment
        FROM `{}.{}.fact_claims` f
        JOIN `{}.{}.dim_provider` p ON f.provider_id = p.provider_id
        GROUP BY p.state
        ORDER BY total_payment DESC
        LIMIT 10
    """.format(PROJECT_ID, DATASET, PROJECT_ID, DATASET)
    df = run_query(query)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/ml-results")
def ml_results():
    results = [
        {"model": "linear_regression", "r2": 0.6486, "rmse": 112.7383, "mae": 40.0027},
        {"model": "random_forest", "r2": 0.6440, "rmse": 113.4785, "mae": 35.1186},
        {"model": "gradient_boosting", "r2": 0.4071, "rmse": 146.4386, "mae": 35.2413},
    ]
    return jsonify(results)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
