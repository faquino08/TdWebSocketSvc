import asyncio
from hypercorn.config import Config
from hypercorn.asyncio import serve

from api import app 

config = Config()
asyncio.run(serve(app,config))