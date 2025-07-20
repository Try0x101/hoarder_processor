from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.routes import internal, data_access, root

app = FastAPI(
    default_response_class=ORJSONResponse,
    title="Hoarder Processor Server",
    docs_url=None, 
    redoc_url=None
)

app.include_router(root.router)
app.include_router(internal.router)
app.include_router(data_access.router)
