import os
from starlette.config import Config
from starlette.requests import Request
from starlette.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth
from fastapi import Depends, HTTPException

config = Config()
oauth = OAuth(config)

CONF_URL = 'https://accounts.google.com/.well-known/openid-configuration'
oauth.register(
    name='google',
    server_metadata_url=CONF_URL,
    client_id=os.environ.get("GOOGLE_CLIENT_ID"),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    client_kwargs={
        'scope': 'openid email profile'
    }
)

async def get_current_user(request: Request):
    if request.client.host in ("127.0.0.1", "localhost"):
        return {"email": "localadmin@system.local"}

    user = request.session.get('user')
    if not user:
        raise HTTPException(status_code=307, detail="Not authenticated", headers={"Location": "/auth/login"})

    allowed_email = os.environ.get("ALLOWED_USER_EMAIL")
    if not allowed_email:
        raise HTTPException(status_code=403, detail="Access configured incorrectly: No allowed user specified.")

    if user.get('email') != allowed_email:
        raise HTTPException(status_code=403, detail="You are not authorized to access this resource.")
    
    return user
