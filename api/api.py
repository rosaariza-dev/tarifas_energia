
import httpx
from config.env import settings

class APIClient:
    def __init__(self, base_url: str, headers: dict | None = None):
        self.client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=30.0
        )

    async def get(self, url: str, params: dict | None = None):
        return await self.client.get(url, params=params)

    async def post(self, url: str, json: dict | None = None):
        return await self.client.post(url, json=json)

    async def close(self):
        await self.client.aclose()

api_tarifas = APIClient(
    base_url=settings.ETL_API,
    headers={
        "Content-Type": "application/json",
        "X-App-Token": settings.APP_TOKEN
    }
)