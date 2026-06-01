from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from routers import pipeline, lake, simulation, insights

app = FastAPI(title="ETL Dashboard API", version="1.0.0", docs_url="/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pipeline.router, prefix="/api/pipeline", tags=["pipeline"])
app.include_router(lake.router,     prefix="/api/lake",     tags=["lake"])
app.include_router(simulation.router, prefix="/api/simulation", tags=["simulation"])
app.include_router(insights.router, prefix="/api/insights", tags=["insights"])

Instrumentator().instrument(app).expose(app)


@app.get("/health")
async def health():
    return {"status": "ok"}
