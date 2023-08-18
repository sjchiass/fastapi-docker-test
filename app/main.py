# app/main.py

from datetime import datetime
from fastapi import FastAPI, Query
from app.db import database, User

import pandas as pd
import json

app = FastAPI(title="Data Logger")


@app.get("/")
async def read_root():
    return await User.objects.all()


@app.on_event("startup")
async def startup():
    if not database.is_connected:
        await database.connect()
    # create a dummy entry
    await User.objects.get_or_create(email="test@test.com")
    query = """CREATE TABLE IF NOT EXISTS sensors (id SERIAL PRIMARY KEY, name VARCHAR(100), variable VARCHAR(100), value FLOAT, datetime TIMESTAMPTZ, auto_datetime BOOLEAN, transaction_time TIMESTAMPTZ)"""
    await database.execute(query=query)


@app.on_event("shutdown")
async def shutdown():
    if database.is_connected:
        await database.disconnect()

@app.get("/log/temperature")
async def log_temp(name: str, value: float, dt: str = None):
    # Insert some data.
    if dt:
        dt = datetime.fromisoformat(dt)
        auto_datetime = False
    else:
        auto_datetime = True
        dt = datetime.now()
    query = "INSERT INTO sensors(name, variable, value, datetime, auto_datetime, transaction_time) VALUES (:name, :variable, :value, :datetime, :auto_datetime, :transaction_time)"
    values = [
        {"name": name, "variable": "temperature",
        "value": value, "datetime": dt,
        "auto_datetime": auto_datetime, "transaction_time": datetime.now()}
    ]
    await database.execute_many(query=query, values=values)

@app.get("/log/humidity")
async def log_temp(name: str, value: float, dt: str = None):
    # Insert some data.
    if dt:
        dt = datetime.fromisoformat(dt)
        auto_datetime = False
    else:
        auto_datetime = True
        dt = datetime.now()
    query = "INSERT INTO sensors(name, variable, value, datetime, auto_datetime, transaction_time) VALUES (:name, :variable, :value, :datetime, :auto_datetime, :transaction_time)"
    values = [
        {"name": name, "variable": "humidity",
        "value": value, "datetime": dt,
        "auto_datetime": auto_datetime, "transaction_time": datetime.now()}
    ]
    await database.execute_many(query=query, values=values)

@app.get("/dump")
async def dump():
    query = "SELECT * FROM public.sensors"
    rows = await database.fetch_all(query=query)
    df = pd.DataFrame([{k:v for k, v in zip(list(row), tuple(row.values()))} for row in rows])
    df.datetime = df.datetime.apply(lambda x: x.isoformat())
    df.transaction_time = df.transaction_time.apply(lambda x: x.isoformat())
    res = df.to_json(orient="records")
    parsed = json.loads(res)
    return parsed

@app.get("/drop")
async def drop():
    query = "DROP TABLE public.sensors"
    await database.execute(query=query)
