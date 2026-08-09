import logging

from dotenv import load_dotenv
from fastapi import FastAPI

from config_definitions import CONFIG_DEFINITIONS
from pipelineFramework import PipelineServer, add_common_api_calls, ConfigurationManager
from pipeline_configs import PIPELINE_CONFIGS

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("pymongo").setLevel(logging.INFO)
load_dotenv()

API_BASE_URL = "/api/admin"

app = FastAPI(
    openapi_url=API_BASE_URL + "/openapi.json", docs_url=API_BASE_URL + "/docs", redoc_url=API_BASE_URL + "/redoc"
)
pipeline_server: PipelineServer = PipelineServer(PIPELINE_CONFIGS, CONFIG_DEFINITIONS)
add_common_api_calls(app, pipeline_server, API_BASE_URL)
