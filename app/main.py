from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.routes import internal, data_access, root, auth
from starlette.middleware.sessions import SessionMiddleware
import os
import asyncio
from app.geojson_processor.processor import run_processor_once

app = FastAPI(
    default_response_class=ORJSONResponse,
    title="Hoarder Processor Server",
    docs_url=None, 
    redoc_url=None
)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(run_processor_once())

app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SESSION_SECRET_KEY", "659c4efed7e605c944c3166acbb6978b35a9eb6a67845a40ac2ab7e73fd2f8ae"),
    https_only=True,
    max_age=86400 * 14
)

app.include_router(root.router)
app.include_router(internal.router)
app.include_router(data_access.router)
app.include_router(auth.router)
