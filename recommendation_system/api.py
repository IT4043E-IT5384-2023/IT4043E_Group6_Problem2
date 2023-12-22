from typing import Union

from fastapi import FastAPI

app = FastAPI()


@app.get("/recommendation/{address}")
def read_item(address: str = None):
    if address is None:
        address = "all"
    return {"address": address, "recommendation": "recommendation"}
