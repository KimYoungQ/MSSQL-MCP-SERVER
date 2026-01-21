import os
from dotenv import load_dotenv

load_dotenv()

# API Gateway configuration
API_GATEWAY_URL = os.getenv("API_GATEWAY_URL", "http://localhost:3000/api/v1")
API_KEY = os.getenv("API_KEY", "")

# Request timeout (seconds)
REQUEST_TIMEOUT = 30.0
