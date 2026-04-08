from fastapi import APIRouter, HTTPException, status
from neo4j import GraphDatabase
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


def get_driver():
    return GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )


class PersonCreate(BaseModel):
    name: str
    city: str
    age: int


class RelationshipCreate(BaseModel):
    from_person: str
    to_person: str
    relationship_type: str = "FRIENDS_WITH"


@s7.post("/graph/person")
def create_person(person: PersonCreate) -> dict:
    """Create a person node in Neo4J."""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            "CREATE (p:Person {name: $name, city: $city, age: $age})",
            name=person.name,
            city=person.city,
            age=person.age,
        )
    driver.close()
    return {"status": "ok", "name": person.name}


@s7.get("/graph/persons")
def list_persons() -> list[dict]:
    """List all person nodes."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run(
            "MATCH (p:Person) RETURN p.name AS name, p.city AS city, p.age AS age"
        )
        persons = [{"name": r["name"], "city": r["city"], "age": r["age"]} for r in result]
    driver.close()
    return persons


@s7.get("/graph/person/{name}/friends")
def get_friends(name: str) -> list[dict]:
    """Get friends of a person (any direction). 404 if not found."""
    driver = get_driver()
    with driver.session() as session:
        check = session.run(
            "MATCH (p:Person {name: $name}) RETURN p", name=name
        )
        if not check.single():
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")

        result = session.run(
            "MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person) "
            "RETURN friend.name AS name, friend.city AS city, friend.age AS age",
            name=name,
        )
        friends = [{"name": r["name"], "city": r["city"], "age": r["age"]} for r in result]
    driver.close()
    return friends


@s7.post("/graph/relationship")
def create_relationship(rel: RelationshipCreate) -> dict:
    """Create FRIENDS_WITH between two persons. 404 if either not found."""
    driver = get_driver()
    with driver.session() as session:
        for person_name in [rel.from_person, rel.to_person]:
            check = session.run(
                "MATCH (p:Person {name: $name}) RETURN p", name=person_name
            )
            if not check.single():
                driver.close()
                raise HTTPException(status_code=404, detail=f"Person '{person_name}' not found")

        session.run(
            "MATCH (a:Person {name: $from_name}), (b:Person {name: $to_name}) "
            "CREATE (a)-[:FRIENDS_WITH]->(b)",
            from_name=rel.from_person,
            to_name=rel.to_person,
        )
    driver.close()
    return {"status": "ok", "from": rel.from_person, "to": rel.to_person}


@s7.get("/graph/person/{name}/recommendations")
def get_recommendations(name: str) -> list[dict]:
    """Friend recommendations: friends-of-friends not already direct friends.
    Sorted by mutual_friends descending. 404 if not found."""
    driver = get_driver()
    with driver.session() as session:
        check = session.run(
            "MATCH (p:Person {name: $name}) RETURN p", name=name
        )
        if not check.single():
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")

        result = session.run(
            "MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend)-[:FRIENDS_WITH]-(rec:Person) "
            "WHERE rec <> p AND NOT (p)-[:FRIENDS_WITH]-(rec) "
            "RETURN rec.name AS name, rec.city AS city, COUNT(friend) AS mutual_friends "
            "ORDER BY mutual_friends DESC",
            name=name,
        )
        recs = [{"name": r["name"], "city": r["city"], "mutual_friends": r["mutual_friends"]} for r in result]
    driver.close()
    return recs