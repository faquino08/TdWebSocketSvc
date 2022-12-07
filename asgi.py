import asyncio
from hypercorn.config import Config
from hypercorn.asyncio import serve

from api import app 

asyncio.run(serve(app,Config()))