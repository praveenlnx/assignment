import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from elasticsearch import AsyncElasticsearch, NotFoundError

app = FastAPI()

# Configuration
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_URL = f"http://{ES_HOST}:{ES_PORT}"

# Global ES client
es = AsyncElasticsearch(ES_URL)
INDEX_NAME = "cities"

class CityPopulation(BaseModel):
    city: str
    population: int

@app.on_event("startup")
async def startup_event():
    # Ensure index exists
    if not await es.indices.exists(index=INDEX_NAME):
        await es.indices.create(index=INDEX_NAME)

@app.on_event("shutdown")
async def shutdown_event():
    await es.close()

@app.get("/health")
async def health_check():
    if await es.ping():
        return {"status": "OK", "elasticsearch": "connected"}
    return {"status": "OK", "elasticsearch": "unreachable"}

@app.put("/api/population")
async def upsert_population(data: CityPopulation):
    doc = data.dict()
    # Use city name lowercased as ID for idempotency
    doc_id = data.city.lower()
    await es.index(index=INDEX_NAME, id=doc_id, document=doc)
    return {"message": "Data saved successfully", "city": data.city, "population": data.population}

@app.get("/api/population/{city_name}")
async def get_population(city_name: str):
    doc_id = city_name.lower()
    try:
        resp = await es.get(index=INDEX_NAME, id=doc_id)
        source = resp['_source']
        return {"city": source['city'], "population": source['population']}
    except NotFoundError:
        raise HTTPException(status_code=404, detail="City not found")
