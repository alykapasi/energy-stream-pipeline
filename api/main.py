# api/main.py

from fastapi import FastAPI

from api.routes import router

app = FastAPI(
    title="Energy Market Intelligence API",
    description="Real-time energy price stream with market context enrichment",
    version="1.0.0",
)

app.include_router(router)

@app.get("/")
def root():
    return {
        "service": "Energy Market Intelligence Pipeline",
        "endpoints": [
            "/prices/latest",
            "/prices/history/{commodity}",
            "/context/{commodity}",
            "/alerts/anomalies",
            "/alerts/impacts",
            "/pipeline/health",
            "/docs",
        ]
    }