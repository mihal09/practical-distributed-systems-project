import requests
import json
from datetime import datetime

def send_user_tag(time, cookie, country, device, action, origin, product_id, brand_id, category_id, price):
    url = "http://localhost:5000/user_tags"
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
    url = f"http://localhost:5000/user_profiles/{cookie}"
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
    if response.status_code == 204:
        print("User profile retrieved succesfully.")
        print(response.text)
    else:
        print(f"Error: Received status code {response.status_code}")
        print(response.text)


if __name__ == "__main__":
    # Example usage
    time = datetime.utcnow().isoformat() + "Z"  # Format the current time in UTC with millisecond precision and 'Z' suffix
    cookie = "example_cookie2"
    country = "US"
    device = "PC"
    action = "VIEW"
    origin = "example_origin"
    product_id = 123
    brand_id = "brand456"
    category_id = "category789"
    price = 999

    send_user_tag(time, cookie, country, device, action, origin, product_id, brand_id, category_id, price)

    # cookie = "example_cookie"
    # time_range = "2024-08-12T18:30:00.000_2024-08-12T23:30:00.000"
    # limit = 2

    # get_user_tag(cookie, time_range, limit)