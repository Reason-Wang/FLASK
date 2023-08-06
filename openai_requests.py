import copy
import json

import openai
from typing import List
from tqdm import tqdm
import time
# import os
# os.environ["HTTPS_PROXY"] = "http://127.0.0.1:10809"
# os.environ["HTTP_PROXY"] = "http://127.0.0.1:10809"

def request_all(api_keys, requests, cache_file):
    num_api_keys = len(api_keys)
    writing_frequency = 20

    results = []
    cache_responses = []
    for index, item in enumerate(tqdm(requests)):
        api_key = api_keys[index % num_api_keys]
        response, success = call_and_return(api_key=api_key, item=item)

        cache_response = copy.deepcopy(response)
        cache_response['success'] = success
        cache_responses.append(cache_response)
        # write cache file
        if index % writing_frequency == writing_frequency - 1:
            with open(cache_file, 'a') as cache_output_file:
                for _cache_response in cache_responses:
                    response_to_write = {
                        'review_id': _cache_response['review_id'],
                        'question_id': _cache_response['question_id'],
                        'response': _cache_response['response'],
                        'success': _cache_response['success']
                    }
                    cache_output_file.write(json.dumps(response_to_write)+'\n')
            cache_responses = []

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
