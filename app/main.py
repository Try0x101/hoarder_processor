from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.routes import internal, data_access, root

app = FastAPI(
    default_response_class=ORJSONResponse,
    title="Hoarder Processor Server",
    docs_url=None, 
    redoc_url=None
)

# Root endpoint for API discovery
app.include_router(root.router)

# Internal endpoint for receiving data from ingest server
app.include_router(internal.router)

# Public endpoints for accessing enriched data
app.include_router(data_access.router)
