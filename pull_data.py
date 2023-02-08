import asyncio
import os
from pathlib import Path
from typing import Dict

import aiofiles
import aiofiles.os
import aiohttp
import tqdm


class EarthdataClient:
    def __init__(self):
        self._client = aiohttp.ClientSession(headers=self._load_headers())
        self._base_url = "https://e4ftl01.cr.usgs.gov"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info) -> bool:
        return await self._client.__aexit__(*exc_info)

    async def fetch_content(self, extension: str) -> bytes:
        async with self._get(extension) as response:
            if response.status == 404:
                raise FileNotFoundError

            assert response.status == 200
            return await response.read()

    def _get(self, extension: str):
        return self._client.get(self._make_url(extension))

    def _make_url(self, extension: str) -> str:
        return f"{self._base_url}/{extension}"

    def _load_headers(self) -> Dict[str, str]:
        return {"Authorization": "Bearer {}".format(self._load_auth_token())}

    def _load_auth_token(self) -> str:
        path = Path.home().joinpath(".config/dressel/earthdata.token")
        with path.open() as f:
            return f.read().strip()


def make_extensions(path):
    with open(path, "r") as f:
        return f.read().splitlines()


async def save_content(content: bytes, path: str):
    await aiofiles.os.makedirs(os.path.dirname(path), exist_ok=True)
    async with aiofiles.open(path, "wb") as f:
        await f.write(content)


async def main():
    save_dir = os.path.expanduser("~/datasets")

    async with EarthdataClient() as client:
        for extension in tqdm.tqdm(make_extensions("extensions.txt")):
            save_path = os.path.join(save_dir, extension)
            if not await aiofiles.os.path.exists(save_path):
                content = await client.fetch_content(extension)
                await save_content(content, save_path)

asyncio.run(main())
