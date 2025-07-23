from fastapi import APIRouter, Request
from starlette.responses import RedirectResponse
from app.security import oauth
from authlib.integrations.base_client.errors import OAuthError

router = APIRouter(prefix="/auth", tags=["Authentication"])

@router.get('/login')
async def login(request: Request):
    redirect_uri = request.url_for('auth_callback')
    return await oauth.google.authorize_redirect(request, redirect_uri)

@router.get('/callback', name='auth_callback')
async def auth_callback(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
    except OAuthError:
        return RedirectResponse(url='/')

    user = token.get('userinfo')
    if user:
        request.session['user'] = dict(user)
    return RedirectResponse(url='/')

@router.get('/logout')
async def logout(request: Request):
    request.session.pop('user', None)
    return RedirectResponse(url='/')
