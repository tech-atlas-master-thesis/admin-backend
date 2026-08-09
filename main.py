import logging

from dotenv import load_dotenv
from fastapi import FastAPI

from config_definitions import CONFIG_DEFINITIONS
from datasets import add_dataset_endpoints
from middleware.requestCancelledMiddleware import RequestCancelledMiddleware
from pipelineFramework import PipelineServer, add_common_api_calls, ConfigurationManager
from pipeline_configs import PIPELINE_CONFIGS

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("pymongo").setLevel(logging.INFO)
load_dotenv()

API_BASE_URL = "/api/admin"

app = FastAPI(
    openapi_url=API_BASE_URL + "/openapi.json", docs_url=API_BASE_URL + "/docs", redoc_url=API_BASE_URL + "/redoc"
)
app.add_middleware(RequestCancelledMiddleware)
# cache = EnrichmentCache(get_cache_db_client())
pipeline_server: PipelineServer = PipelineServer(PIPELINE_CONFIGS, CONFIG_DEFINITIONS)
add_common_api_calls(app, pipeline_server, API_BASE_URL)
add_dataset_endpoints(app, API_BASE_URL)
