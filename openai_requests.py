import openai
from typing import List
from tqdm import tqdm
import time
# import os
# os.environ["HTTPS_PROXY"] = "http://127.0.0.1:10809"
# os.environ["HTTP_PROXY"] = "http://127.0.0.1:10809"

def request_all(api_keys, requests):
    num_api_keys = len(api_keys)

    results = []
    for index, item in enumerate(tqdm(requests)):
        api_key = api_keys[index % num_api_keys]
        response, success = call_and_return(api_key=api_key, item=item)
        results.append((response, success))

    responses = []
    fails = []
    for result in results:
        response, success = result
        if success:
            responses.append(response)
        else:
            fails.append(response)

    return responses, fails


def call_and_return(api_key: str, item: dict, num_retries=5):
    openai.api_key = api_key
    success = False
    response = None
    for _ in range(num_retries):
        try:
            response = openai.ChatCompletion.create(**item["request"])
            success = True
            break
        except openai.error.OpenAIError as exception:
            print(f"{exception}. Retrying ...")
            time.sleep(3)

    output_item = {**item, "api_key": api_key, "response": response}

    return output_item, success
