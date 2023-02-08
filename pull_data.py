import asyncio
import logging
import os
from io import BytesIO
from pathlib import Path
from typing import AsyncIterator, Dict

import aiofiles
import aiofiles.os
import aiohttp
from PIL import Image
from tqdm import tqdm

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("dressel")


class EarthdataClient:
    def __init__(self):
        self._client = aiohttp.ClientSession(headers=self._load_headers())
        self._base_url = "https://e4ftl01.cr.usgs.gov"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info) -> bool:
        return await self._client.__aexit__(*exc_info)

    async def fetch_image(self, extension: str) -> Image.Image:
        async with self._get(extension) as response:
            if response.status == 404:
                raise FileNotFoundError

            assert response.status == 200
            raw_data = await response.read()
            buff = BytesIO(raw_data)
            return Image.open(buff)

    async def fetch_annotation(self, extension: str) -> str:
        async with self._get(extension) as response:
            if response.status == 404:
                raise FileNotFoundError

            assert response.status == 200
            return await response.text()

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


async def save_image(image: Image.Image, path: str):
    await aiofiles.os.makedirs(os.path.dirname(path), exist_ok=True)
    async with aiofiles.open(path, "wb") as f:
        image.save(f, format="JPEG")


async def save_annotation(annotation: str, path: str):
    await aiofiles.os.makedirs(os.path.dirname(path), exist_ok=True)
    async with aiofiles.open(path, "w") as f:
        await f.write(annotation)


def make_extensions(images, annotations):
    with open(images, "r") as fi:
        with open(annotations, "r") as fd:
            return list(zip(fi.read().splitlines(), fd.read().splitlines()))


async def main():
    save_dir = os.path.expanduser("~/datasets")
    extensions = make_extensions("images.txt", "annotations.txt")

    async with EarthdataClient() as client:
        for image_ext, annotation_ext in tqdm(extensions):
            image = await client.fetch_image(image_ext)
            save_path = os.path.join(save_dir, image_ext)
            await save_image(image, save_path)

            annotation = await client.fetch_annotation(annotation_ext)
            save_path = os.path.join(save_dir, annotation_ext)
            await save_annotation(annotation, save_path)


asyncio.run(main())
