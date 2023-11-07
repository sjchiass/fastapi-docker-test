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


@app.get("/report/", response_class=HTMLResponse)
async def report():
    # Set numeric precision
    pd.set_option("display.precision", 2)
    query = notes.select()
    results = await database.fetch_all(query)
    df = pd.DataFrame(results)
    # Localize datetime
    df["create_date"] = pd.to_datetime(df["create_date"], utc=True).dt.tz_localize("America/Toronto", ambiguous="infer")
    # Measurement stats
    first_dt = df["create_date"].min()
    latest_dt = df["create_date"].max()
    last_24h = df[df["create_date"] >= latest_dt-timedelta(hours=24)].groupby(["location", "sensor", "measurand", "units"]).value.describe().to_html()
    last_24h_per_hour = (df
                         [df["create_date"] >= latest_dt-timedelta(hours=24)]
                         .groupby(["location", "sensor", "measurand", "units", df.create_date.dt.round("1h")])
                         .value
                         .mean()
                         .reset_index()
                         .rename(columns={"create_date" : "hour"})
                         .pivot(index=["location", "sensor", "measurand", "units"], columns="hour", values="value")
                         .to_html()
                         )
    all_time = df.groupby(["location", "sensor", "measurand", "units"]).value.describe().to_html()
    all_time_per_hour = (df
                         .groupby(["location", "sensor", "measurand", "units", df.create_date.dt.hour])
                         .value
                         .mean()
                         .reset_index()
                         .rename(columns={"create_date" : "hour"})
                         .pivot(index=["location", "sensor", "measurand", "units"], columns="hour", values="value")
                         .to_html()
                         )
    # Attempt a table of links
    links = (df
             [["location", "sensor", "measurand", "units"]]
             .drop_duplicates()
             .sort_values(["location", "sensor", "measurand", "units"])
            )
    links["time series graph"] = links.apply(lambda df: f"<a href='./time_series_graph?location={df.location}&sensor={df.sensor}&measurand={df.measurand}&units={df.units}&dt_grouping=1d'>link</a>", axis=1)
    links["time series graph, 7d"] = links.apply(lambda df: f"<a href='./time_series_graph?location={df.location}&sensor={df.sensor}&measurand={df.measurand}&units={df.units}&last_days=7&dt_grouping=1h'>link</a>", axis=1)
    links["hourly range graph"] = links.apply(lambda df: f"<a href='./hourly_range_graph?location={df.location}&sensor={df.sensor}&measurand={df.measurand}&units={df.units}'>link</a>", axis=1)
    links = links.set_index(["location", "sensor", "measurand", "units"])[["time series graph", "time series graph, 7d", "hourly range graph"]]
    # Attempt a table of common measurands
    multi_measurands = (df
             [["location", "sensor", "measurand", "units"]]
             .drop_duplicates()
             .sort_values(["location", "sensor", "measurand", "units"])
             .groupby(["measurand", "units"])
             .count()
             .reset_index()
             .loc[lambda df: (df.location > 1) & (df.location > 1)]
             [["measurand", "units"]]
            )
    multi_measurands["time series graph"] = multi_measurands.apply(lambda df: f"<a href='./time_series_graph?measurand={df.measurand}&units={df.units}&dt_grouping=1d'>link</a>", axis=1)
    multi_measurands["time series graph, 7d"] = multi_measurands.apply(lambda df: f"<a href='./time_series_graph?measurand={df.measurand}&units={df.units}&last_days=7&dt_grouping=1h'>link</a>", axis=1)
    multi_measurands = multi_measurands.set_index(["measurand", "units"])[["time series graph", "time series graph, 7d"]]
    return f"<h1>Last 24 hours</h1><h3>Latest measurement {latest_dt}</h3>{last_24h}<br>{last_24h_per_hour}<h1>All time</h1><h3>Since {first_dt}</h3>{all_time}<br>{all_time_per_hour}<h1>Graphs</h1>{links.to_html(render_links=True, escape=False)}<h3>Combined</h3>{multi_measurands.to_html(render_links=True, escape=False)}"

@app.get("/report/time_series_graph", responses = {200: {"content": {"image/png": {}}}}, response_class=Response)
async def time_series_graph(measurand: str, units:str, location: str | None = None, sensor: str | None = None, last_days: int | None = None, dt_grouping: str = "1h"):
    query = notes.select()
    results = await database.fetch_all(query)
    df = pd.DataFrame(results)
    # Localize datetime
    df["create_date"] = pd.to_datetime(df["create_date"], utc=True).dt.tz_localize("America/Toronto", ambiguous="infer")
    if last_days is not None:
        latest_dt = df["create_date"].max()
        df = df.loc[df["create_date"] >= latest_dt-timedelta(days=last_days)]
    if location is None and sensor is None:
        # Filter for specific sensor readings
        filtered = (df
                .loc[(df.measurand == measurand) & (df.units == units)]
            )
        filtered["identifier"] = filtered.apply(lambda df: f"{df['location']}, {df['sensor']}", axis=1)
        # Average observations by the hour
        if dt_grouping is not None:
            filtered = (filtered
            .groupby(["identifier", df.create_date.dt.round(dt_grouping)])
            .value
            .mean()
            .reset_index()
            )
        for identifier in filtered.identifier.drop_duplicates().values:
            # A simple standard time series plot
            plt.plot(filtered.loc[filtered.identifier == identifier].create_date,
                filtered.loc[filtered.identifier == identifier].value,
                label=identifier)
        plt.title(f"{measurand.title()}")
        plt.legend()
    else:
        # Filter for specific sensor readings
        filtered = (df
                .loc[(df.location == location) & (df.sensor == sensor) & (df.measurand == measurand) & (df.units == units)]
            )
        # Average observations by the hour
        if dt_grouping is not None:
            filtered = (filtered
            .groupby(["location", "sensor", "measurand", "units", df.create_date.dt.round(dt_grouping)])
            .value
            .mean()
            .reset_index()
            )
        # A simple standard time series plot
        plt.plot(filtered.create_date, filtered.value)
        plt.title(f"{measurand.title()} from {sensor} in {location}")
    plt.xlabel("Time")
    # Use this method to auto-format the x axis for dates
    plt.gcf().autofmt_xdate()
    plt.ylabel(units.title())
    plt.show()

    with io.BytesIO() as buffer:  # use buffer memory
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        image = buffer.getvalue()
    # Close the figure so that multiple requests don't keep overlapping
    plt.close()
    return Response(content=image, media_type="image/png")

@app.get("/report/hourly_range_graph", responses = {200: {"content": {"image/png": {}}}}, response_class=Response)
async def hourly_range_graph(location: str, sensor: str, measurand: str, units: str):
    query = notes.select()
    results = await database.fetch_all(query)
    df = pd.DataFrame(results)
    # Localize datetime
    df["create_date"] = pd.to_datetime(df["create_date"], utc=True).dt.tz_localize("America/Toronto", ambiguous="infer")
    # Filter for specific sensor readings
    filtered = (df
            .loc[(df.location == location) & (df.sensor == sensor) & (df.measurand == measurand) & (df.units == units)]
           )
    # Determine the last data point
    latest_dt = filtered["create_date"].max()
    # Create a convenience function for aggregating by hour
    def by_hour(df, func="mean"):
        return (df
                .groupby(["location", "sensor", "measurand", "units", df.create_date.dt.hour])
                .value
                .aggregate(func)
                .reset_index()
                .rename(columns={"create_date" : "hour"})
               )
    # The current day, likely partial
    today = by_hour(filtered[filtered["create_date"].dt.day == latest_dt.day])
    # Yesterday, which should cover a period of 24 hours, assuming 100% uptime
    yesterday = by_hour(filtered[filtered["create_date"].dt.day == (latest_dt-timedelta(days=1)).day])
    # Before yesterday, calculate standard range around the mean
    before = filtered[filtered["create_date"].dt.day < (latest_dt-timedelta(days=1)).day]
    before_q25 = by_hour(before, lambda x: x.quantile(0.25))
    before_q75 = by_hour(before, lambda x: x.quantile(0.75))

    # Standard line plot with a shaded area for the standard range
    plt.plot(today.hour, today.value, label="Today")
    plt.plot(yesterday.hour, yesterday.value, label="Yesterday")
    plt.fill_between(before_q25.hour,
                     before_q25.value,
                     before_q75.value,
                     alpha=0.2,
                     label="Interquartile Range"
                    )
    plt.legend()
    plt.title(f"{measurand.title()} from {sensor} in {location}")
    plt.xlabel("Hour")
    plt.ylabel(units.title())
    #plt.show()

    with io.BytesIO() as buffer:  # use buffer memory
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        image = buffer.getvalue()
    # Close the figure so that multiple requests don't keep overlapping
    plt.close()
    return Response(content=image, media_type="image/png")

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


