import string
import asyncio
import logging
import httpx
from concurrent.futures import ThreadPoolExecutor

from collections import defaultdict, Counter
from matplotlib import pyplot as plt

async def get_text(url: str) -> str:
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        if response.status_code == 200:
            return response.text
        else:
            logging.error("Connection error")
            raise ValueError(f"Error {response.status_code} on {url}")

def remove_punctuation(text: str) -> str:
    return text.translate(str.maketrans("", "", string.punctuation))

def map_function(word: str) -> tuple:
    return word, 1

def shuffle_function(mapped_values: list) -> tuple:
    shuffle_values = defaultdict(list)
    for values in mapped_values:
        key, value = values
        shuffle_values[key].append(value)
    return shuffle_values.items()

def reduce_function(key_value: tuple) -> tuple:
    key, value = key_value
    return key, len(value)

async def map_reduce_function(url: str) -> dict:
    try:
        text = await get_text(url)
        text = remove_punctuation(text)
        words = text.split()

        with ThreadPoolExecutor() as executor:
            mapped_values = list(
                executor.map(map_function, [word for word in words])
            )

        shuffled_values = shuffle_function(mapped_values)

        with ThreadPoolExecutor() as executor:  
            reduced_values = dict(
                executor.map(reduce_function, [values for values in shuffled_values])
            )

        return reduced_values
    except ValueError as e:
        logging.error("Map reduce error")
        raise ValueError(e)

def visualize_top_words(res: dict) -> None:
    try:
        top_10 = Counter(res).most_common(10)
        labels, values = zip(*top_10)
        plt.figure(figsize=(10, 5))
        plt.barh(labels, values, color=["#99CCFF"])
        plt.xlabel("Quantity")
        plt.ylabel("Word")
        plt.title("Top10 most frequent words")

        plt.gca().invert_yaxis()
        plt.show()
    except Exception as e:
        logging.error("Plotting error")
        raise Exception(e)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    try:
        res = asyncio.run(
            map_reduce_function("https://www.gutenberg.org/files/1342/1342-0.txt")
        )
        visualize_top_words(res)
    except Exception as e:
        logging.error(e)
