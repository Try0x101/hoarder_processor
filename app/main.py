from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.routes import internal, data_access, root, auth
from starlette.middleware.sessions import SessionMiddleware
import os
import asyncio
import aiosqlite
from app.database import DB_PATH, get_all_oui_vendors, initialize_database_if_needed
from app.utils import OUI_VENDOR_MAP

app = FastAPI(
    default_response_class=ORJSONResponse,
    title="Hoarder Processor Server",
    docs_url=None, 
    redoc_url=None
)

@app.on_event("startup")
async def startup_event():
    try:
        await asyncio.to_thread(initialize_database_if_needed)
        async with aiosqlite.connect(f"file:{DB_PATH}?mode=ro", uri=True) as db:
            OUI_VENDOR_MAP.update(await get_all_oui_vendors(db))
            if OUI_VENDOR_MAP:
                print(f"Successfully loaded {len(OUI_VENDOR_MAP)} OUI vendors into memory.")
            else:
                print("CRITICAL: OUI vendor map is empty after startup. MAC address lookups will be slow or fail.")
    except Exception as e:
        print(f"CRITICAL: Failed to load OUI vendors from database on startup: {e}")

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
