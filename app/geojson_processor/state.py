import orjson
import aiofiles
from . import settings

async def load_last_processed_id() -> int:
    try:
        async with aiofiles.open(settings.STATE_FILE_PATH, mode='r') as f:
            content = await f.read()
            state_data = orjson.loads(content)
            return state_data.get("last_processed_id", 0)
    except (FileNotFoundError, orjson.JSONDecodeError, KeyError):
        return 0

async def save_last_processed_id(last_id: int):
    try:
        async with aiofiles.open(settings.STATE_FILE_PATH, mode='w') as f:
            await f.write(orjson.dumps({"last_processed_id": last_id}).decode())
    except IOError:
        pass
