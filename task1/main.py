import argparse
import asyncio
import logging

from typing import Coroutine, Any

from aiopath import AsyncPath
from aioshutil import copyfile

parser = argparse.ArgumentParser(description='Copy files from one directory to another and sort them.')
parser.add_argument('--source', "-s", help='Source directory', required=True)
parser.add_argument("--output", "-o", help='Output directory', default='output_folder')

print(parser.parse_args())
args = vars(parser.parse_args())

source = AsyncPath(args.get('source'))
output = AsyncPath(args.get('output'))

async def read_folder (path: AsyncPath) -> Coroutine[Any, Any, None]:
    try:
        async for file in path.iterdir():
            if await file.is_dir():
                await read_folder(file)
            else:
                await copy_files(file)
    except FileNotFoundError as e:
        logging.error(e)

async def copy_files(file: AsyncPath) -> Coroutine[Any, Any, None]:
    new_folder = output / file.suffix[1:]
    try:
        await new_folder.mkdir(exist_ok=True, parents=True)
        await copyfile(file, new_folder / file.name)
    except OSError as e:
        logging.error(e)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    asyncio.run(read_folder(source))
