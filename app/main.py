# app/main.py
import sqlite3
sqlite3.connect("/sqlite/test.db")
from datetime import datetime

from typing import List, Optional

import databases
import sqlalchemy
from fastapi import FastAPI
from pydantic import BaseModel, Field

# SQLAlchemy specific code, as with any other app
DATABASE_URL = "sqlite:////sqlite/test.db"
# DATABASE_URL = "postgresql://user:password@postgresserver/db"

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("create_date", sqlalchemy.DateTime),
    sqlalchemy.Column("sensor", sqlalchemy.String),
    sqlalchemy.Column("reading", sqlalchemy.Float),
)


engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
metadata.create_all(engine)


class NoteIn(BaseModel):
    sensor: str
    reading: float
    create_date: datetime = datetime.now()

class Note(BaseModel):
    id: int
    sensor: str
    reading: float
    create_date: datetime


app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/notes/", response_model=List[Note])
async def read_notes():
    query = notes.select()
    return await database.fetch_all(query)


@app.post("/notes/", response_model=Note)
async def create_note(note: NoteIn):
    query = notes.insert().values(create_date=note.create_date, sensor=note.sensor, reading=note.reading)
    last_record_id = await database.execute(query)
    return {**note.dict(), "id": last_record_id}
