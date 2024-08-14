import requests
import json
from datetime import datetime
import random

base_url = "http://st108vm105.rtb-lab.pl"

def send_user_tag(time, cookie, country, device, action, origin, product_id, brand_id, category_id, price):
    url = f"{base_url}:5000/user_tags"
    headers = {
        "Content-Type": "application/json"
    }

    # Construct the JSON payload
    user_tag = {
        "time": time,  # Example: "2022-03-22T12:15:00.000Z"
        "cookie": cookie,
        "country": country,
        "device": device,
        "action": action,
        "origin": origin,
        "product_info": {
            "product_id": product_id,
            "brand_id": brand_id,
            "category_id": category_id,
            "price": price
        }
    }

    # Send the POST request
    response = requests.post(url, headers=headers, data=json.dumps(user_tag))

    # Check the response status
    if response.status_code == 204:
        print("User tag added successfully.")
    else:
        print(f"Error: Received status code {response.status_code}")
        print(response.text)


def get_user_tag(cookie, time_range, limit=200):
    url = f"{base_url}:5000/user_profiles/{cookie}"
    headers = {
        "Content-Type": "application/json"
    }

    params = {
        'time_range': time_range,
        'limit': limit
    }


    # Send the POST request
    response = requests.post(url, headers=headers, params=params, data=json.dumps(params))

    # Check the response status
    if response.status_code == 200:
        print("User profile retrieved succesfully.")
        print(response.text)
    else:
        print(f"Error: Received status code {response.status_code}")
        print(response.text)


def get_aggregates(time_range, action="VIEW", limit=200):
    url = f"{base_url}:5000/aggregates"
    headers = {
        "Content-Type": "application/json"
    }

    params = {
        'time_range': time_range,
        'action': action,
        'aggregates': ["COUNT", "SUM_PRICE"],
        'brand_id': "brand456"
    }


    # Send the POST request
    response = requests.post(url, headers=headers, params=params, data=json.dumps(params))

    # Check the response status
    if response.status_code == 204:
        print("Aggregates read succesfully.")
        print(response.text)
    else:
        print(f"Error: Received status code {response.status_code}")
        print(response.text)


if __name__ == "__main__":
    # Example usage
    # time = datetime.utcnow().isoformat() + "Z"  # Format the current time in UTC with millisecond precision and 'Z' suffix
    # time = "2024-08-12T18:39:00.000Z"
    # cookie = "new_cookie"
    # country = "US"
    # device = "PC"
    # action = "VIEW"
    # origin = "example_origin"
    # product_id = 123
    # brand_id = "brand456"
    # category_id = "category789"
    # price = 10

    # for i in range(2):
    #     send_user_tag(time, cookie, country, device, action, origin, product_id, brand_id, category_id, price)


    # time_range = "2024-08-12T18:30:00_2024-08-12T18:31:00"
    # get_aggregates(time_range)

    # cookie = "test_cookie2"
    # time_range = "2024-08-12T18:30:00.000_2024-08-12T23:30:00.000"
    # limit = 2
    # get_user_tag(cookie, time_range, limit)


    origins = ["example_origin1", "example_origin2", "example_origin3"]
    product_ids = [123, 234, 345, 456]
    brand_ids = ["brand456"]
    category_ids = ["category789", "category012", "category345"]

    cookie = "new_cookie"
    country = "US"
    device = "PC"
    action = "VIEW"
    price = 10

    for i in range(100):
        x = random.randint(0, 59)
        time = f"2024-08-12T18:39:{x:02d}.000Z"
        origin = random.choice(origins)
        product_id = random.choice(product_ids)
        brand_id = random.choice(brand_ids)
        category_id = random.choice(category_ids)
        send_user_tag(time, cookie, country, device, action, origin, product_id, brand_id, category_id, price)

