import os

OUTPUT_DIR = "/opt/hoarder_processor/geojson_output"
STATE_FILE_PATH = "/opt/hoarder_processor/geojson_processor_state.json"
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "hoarder_processor.db"))

MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024
QUERY_BATCH_SIZE = 500
TASK_SCHEDULE_SECONDS = 300
