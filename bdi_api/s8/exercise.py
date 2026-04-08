import sqlite3
from pathlib import Path

from fastapi import APIRouter, status
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

DB_PATH = Path(__file__).parent.parent.parent / "data" / "s8" / "aircraft.db"

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)


class AircraftReturn(BaseModel):
    icao: str
    registration: str | None
    type: str | None
    owner: str | None
    manufacturer: str | None
    model: str | None


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: float | None


def get_db():
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all aircraft with enriched data, ordered by ICAO ascending."""
    conn = get_db()
    if not conn:
        return []

    offset = page * num_results
    rows = conn.execute(
        "SELECT icao, registration, type, owner, manufacturer, model "
        "FROM aircraft ORDER BY icao ASC LIMIT ? OFFSET ?",
        (num_results, offset),
    ).fetchall()
    conn.close()

    return [
        AircraftReturn(
            icao=r["icao"],
            registration=r["registration"],
            type=r["type"],
            owner=r["owner"],
            manufacturer=r["manufacturer"],
            model=r["model"],
        )
        for r in rows
    ]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    """Calculate CO2 emissions for a given aircraft on a specific day."""
    conn = get_db()
    if not conn:
        return AircraftCO2Return(icao=icao, hours_flown=0.0, co2=None)

    # Count observations
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM observations WHERE icao = ? AND day = ?",
        (icao.lower(), day),
    ).fetchone()
    num_observations = row["cnt"] if row else 0

    hours_flown = (num_observations * 5) / 3600

    # Get aircraft type
    ac_row = conn.execute(
        "SELECT type FROM aircraft WHERE icao = ?", (icao.lower(),)
    ).fetchone()
    aircraft_type = ac_row["type"] if ac_row and ac_row["type"] else None

    co2 = None
    if aircraft_type:
        fuel_row = conn.execute(
            "SELECT galph FROM fuel_rates WHERE type_code = ?", (aircraft_type,)
        ).fetchone()
        if fuel_row:
            galph = fuel_row["galph"]
            fuel_used_kg = hours_flown * galph * 3.04
            co2 = (fuel_used_kg * 3.15) / 907.185

    conn.close()
    co2_value = round(co2, 4) if co2 is not None else None
    return AircraftCO2Return(icao=icao, hours_flown=round(hours_flown, 4), co2=co2_value)
