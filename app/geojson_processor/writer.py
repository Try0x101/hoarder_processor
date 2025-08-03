import os
import re
import orjson
import aiofiles
import asyncio
import shutil
import datetime
from typing import Dict, Any, Optional, List
from . import settings

def _format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes}B"
    if size_bytes < 1024 * 1024:
        return f"{round(size_bytes / 1024)}KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{round(size_bytes / (1024 * 1024), 1)}MB"
    return f"{round(size_bytes / (1024 * 1024 * 1024), 1)}GB"

class GeoJSONManager:
    def __init__(self):
        self.latest_file_path = self._find_latest_file()
        self._file_handle = None
        self._temp_path = None
        self._is_first_feature = True
        self._features_written = False
        self.timestamp_str = None
        self.started_from_existing = False

    def _find_latest_file(self) -> Optional[str]:
        try:
            files = [os.path.join(settings.OUTPUT_DIR, f) for f in os.listdir(settings.OUTPUT_DIR) if f.startswith("hoarder_") and f.endswith(".geojson")]
            return max(files, key=os.path.getctime) if files else None
        except (FileNotFoundError, OSError):
            return None

    async def start_writing(self):
        os.makedirs(settings.OUTPUT_DIR, exist_ok=True)
        self.timestamp_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        self._temp_path = os.path.join(settings.OUTPUT_DIR, f"hoarder_{self.timestamp_str}.{os.getpid()}.tmp")

        if self.latest_file_path and os.path.getsize(self.latest_file_path) > 20 and os.path.getsize(self.latest_file_path) < settings.MAX_FILE_SIZE_BYTES:
            try:
                await asyncio.to_thread(shutil.copyfile, self.latest_file_path, self._temp_path)
                self.started_from_existing = True
                size = os.path.getsize(self._temp_path)

                self._file_handle = await aiofiles.open(self._temp_path, "r+b")
                
                await self._file_handle.seek(size - 2, os.SEEK_SET)
                if (await self._file_handle.read(2)).strip() == b']}':
                    await self._file_handle.seek(size - 3, os.SEEK_SET)
                    last_char = await self._file_handle.read(1)
                    is_empty_array = last_char == b'['
                    
                    await self._file_handle.seek(size - 2, os.SEEK_SET)
                    await self._file_handle.truncate()
                    
                    self._is_first_feature = is_empty_array
                    return
            except Exception:
                self.started_from_existing = False
                if self._file_handle:
                    await self._file_handle.close()
                    self._file_handle = None

        self._file_handle = await aiofiles.open(self._temp_path, "wb")
        await self._file_handle.write(b'{"type":"FeatureCollection","features":[')
        self._is_first_feature = True

    async def write_features(self, features: List[Dict[str, Any]]):
        if not self._file_handle or not features:
            return
        self._features_written = True
        for feature in features:
            prefix = b'' if self._is_first_feature else b','
            await self._file_handle.write(prefix + orjson.dumps(feature))
            self._is_first_feature = False

    async def finalize(self):
        if not self._file_handle:
            print("GEOJSON Writer: Finalize called but no file handle exists. Aborting.")
            return

        await self._file_handle.write(b']}')
        await self._file_handle.close()
        self._file_handle = None

        if not self._features_written and not self.started_from_existing:
            print(f"GEOJSON Writer: No new features written and not continuing an existing file. Deleting temp file: {self._temp_path}")
            try:
                os.remove(self._temp_path)
            except OSError as e:
                print(f"GEOJSON Writer: Error deleting temp file {self._temp_path}: {e}")
            return

        print(f"GEOJSON Writer: Finalizing GeoJSON file. Features written: {self._features_written}. Continued from existing: {self.started_from_existing}.")
        try:
            file_size = os.path.getsize(self._temp_path)
            size_str = _format_size(file_size)
            
            new_filename = f"hoarder_{self.timestamp_str}_{size_str}.geojson"
            final_path = os.path.join(settings.OUTPUT_DIR, new_filename)

            os.rename(self._temp_path, final_path)

            if self.latest_file_path and self.latest_file_path != final_path and os.path.exists(self.latest_file_path):
                os.remove(self.latest_file_path)
        except (OSError, AttributeError):
            if os.path.exists(self._temp_path):
                 os.rename(self._temp_path, f"{self._temp_path}.geojson")
        finally:
            self._temp_path = None
