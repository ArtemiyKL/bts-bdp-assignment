from typing import Annotated

from fastapi import APIRouter, status, HTTPException
from fastapi.params import Query
from pydantic import BaseModel
from pymongo import MongoClient

from bdi_api.settings import Settings

settings = Settings()

s6 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s6",
    tags=["s6"],
)

class AircraftPosition(BaseModel):
    icao: str
    registration: str | None = None
    type: str | None = None
    lat: float
    lon: float
    alt_baro: float | None = None
    ground_speed: float | None = None
    timestamp: str


# Helper function to get the MongoDB collection dynamically
def get_collection():
    client = MongoClient(settings.mongo_url)
    db = client["bdi_aircraft"]
    return db["positions"]


@s6.post("/aircraft")
def create_aircraft(position: AircraftPosition) -> dict:
    """Store an aircraft position document in MongoDB."""
    collection = get_collection()
    # Convert Pydantic model to a dict and insert
    collection.insert_one(position.model_dump())
    return {"status": "ok"}


@s6.get("/aircraft/stats")
def aircraft_stats() -> list[dict]:
    """Return aggregated statistics: count of positions grouped by aircraft type."""
    collection = get_collection()
    pipeline = [
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}, # Sort descending
        {"$project": {"type": "$_id", "count": 1, "_id": 0}}
    ]
    return list(collection.aggregate(pipeline))


@s6.get("/aircraft/")
def list_aircraft(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    page_size: Annotated[
        int,
        Query(description="Number of results per page", ge=1, le=100),
    ] = 20,
) -> list[dict]:
    """List all aircraft with pagination."""
    collection = get_collection()
    skip_amount = (page - 1) * page_size
    
    # Use aggregation to get distinct aircraft and project required fields
    pipeline = [
        {"$group": {
            "_id": "$icao", 
            "icao": {"$first": "$icao"}, 
            "registration": {"$first": "$registration"}, 
            "type": {"$first": "$type"}
        }},
        {"$project": {"_id": 0}}, # Hide Mongo ID
        {"$sort": {"icao": 1}},   # Consistent sorting for pagination
        {"$skip": skip_amount},
        {"$limit": page_size}
    ]
    
    return list(collection.aggregate(pipeline))


@s6.get("/aircraft/{icao}")
def get_aircraft(icao: str) -> dict:
    """Get the latest position data for a specific aircraft."""
    collection = get_collection()
    
    # Sort by timestamp descending (-1) to get the latest position
    doc = collection.find_one({"icao": icao}, {"_id": 0}, sort=[("timestamp", -1)])
    
    if not doc:
        # Return 404 if not found
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Aircraft not found")
        
    return doc


@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Remove all position records for an aircraft."""
    collection = get_collection()
    result = collection.delete_many({"icao": icao})
    
    # Return the exact number of deleted documents
    return {"deleted": result.deleted_count}