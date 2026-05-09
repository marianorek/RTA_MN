from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np

app = FastAPI(title="Fraud Detection API — Random Forest")

model = pickle.load(open('fraud_model.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    hour: int
    is_electronics: int
    tx_per_day: int

@app.post("/score")
def score(tx: Transaction):
    X = np.array([[tx.amount, tx.hour, tx.is_electronics, tx.tx_per_day]])
    is_fraud = bool(model.predict(X)[0])
    fraud_probability = float(model.predict_proba(X)[0][1])
    return {
        "is_fraud":          is_fraud,
        "fraud_probability": round(fraud_probability, 4),
        "model":             "random_forest",
    }

@app.get("/health")
def health():
    return {"status": "ok"}
