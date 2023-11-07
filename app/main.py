# app/main.py
import sqlite3
sqlite3.connect("/sqlite/test.db")
from datetime import datetime, timedelta

from typing import List, Optional

import databases
import sqlalchemy
from fastapi import FastAPI
from fastapi.responses import Response, HTMLResponse
from pydantic import BaseModel, Field

import pandas as pd
import io
import matplotlib.pyplot as plt

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
    sqlalchemy.Column("location", sqlalchemy.String),
    sqlalchemy.Column("sensor", sqlalchemy.String),
    sqlalchemy.Column("measurand", sqlalchemy.String),
    sqlalchemy.Column("units", sqlalchemy.String),
    sqlalchemy.Column("value", sqlalchemy.Float),
    sqlalchemy.Column("uptime_at_measure", sqlalchemy.Integer),
    sqlalchemy.Column("uptime_at_transmit", sqlalchemy.Integer),
)


engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
metadata.create_all(engine)


class NoteIn(BaseModel):
    location: str
    sensor: str
    measurand: str
    units: str
    value: float
    uptime_at_measure: int
    uptime_at_transmit: int
    create_date: datetime = Field(default_factory=datetime.now)

class Note(BaseModel):
    id: int
    location: str
    sensor: str
    measurand: str
    units: str
    value: float
    uptime_at_measure: int
    uptime_at_transmit: int
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

# https://docs.sqlalchemy.org/en/20/tutorial/data_select.html#the-select-sql-expression-construct
@app.get("/distinct/")
async def distinct():
    query = sqlalchemy.sql.select(notes.c.location, notes.c.sensor, notes.c.measurand, notes.c.units).distinct()
    return await database.fetch_all(query)

@app.get("/filter/")
async def filter(
    location: str | None = None,
    sensor: str | None = None,
    measurand: str | None = None,
    units: str | None = None,
    datetime_le: datetime | None = None,
    datetime_ge: datetime | None = None):
    query = notes.select()
    if location:
        query = query.where(notes.c.location == location)
    if sensor:
        query = query.where(notes.c.sensor == sensor)
    if measurand:
        query = query.where(notes.c.measurand == measurand)
    if units:
        query = query.where(notes.c.units == units)
    if datetime_ge:
        query = query.where(notes.c.create_date >= datetime_ge)
    if datetime_le:
        query = query.where(notes.c.create_date <= datetime_le)

    return await database.fetch_all(query)

@app.post("/notes/", response_model=Note)
async def create_note(note: NoteIn):
    query = notes.insert().values(create_date=note.create_date,
                                  location=note.location,
                                  sensor=note.sensor,
                                  measurand=note.measurand,
                                  units=note.units,
                                  value=note.value,
                                  uptime_at_measure=note.uptime_at_measure,
                                  uptime_at_transmit=note.uptime_at_transmit)
    last_record_id = await database.execute(query)
    return {**note.dict(), "id": last_record_id}


