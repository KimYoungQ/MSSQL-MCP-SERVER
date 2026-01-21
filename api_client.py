import httpx
from typing import Any, Optional
from config import API_GATEWAY_URL, API_KEY, REQUEST_TIMEOUT


class APIClient:
    """HTTP client for communicating with the API Gateway"""

    def __init__(self):
        self.base_url = API_GATEWAY_URL
        self.headers = {
            "X-API-Key": API_KEY,
            "Content-Type": "application/json"
        }

    async def get(self, endpoint: str) -> Optional[dict[str, Any]]:
        """Execute GET request to API Gateway"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}{endpoint}",
                    headers=self.headers,
                    timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                return {"error": f"HTTP {e.response.status_code}: {e.response.text}"}
            except httpx.ConnectError:
                return {"error": "Cannot connect to API Gateway. Is it running?"}
            except Exception as e:
                return {"error": str(e)}

    async def post(self, endpoint: str, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Execute POST request to API Gateway"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self.base_url}{endpoint}",
                    headers=self.headers,
                    json=data,
                    timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                return {"error": f"HTTP {e.response.status_code}: {e.response.text}"}
            except httpx.ConnectError:
                return {"error": "Cannot connect to API Gateway. Is it running?"}
            except Exception as e:
                return {"error": str(e)}


# Singleton instance
api_client = APIClient()
