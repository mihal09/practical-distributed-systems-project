from connections import AerospikeClient


if __name__ == "__main__":
    client = AerospikeClient()

    # time = datetime.utcnow().isoformat() + "Z"  # Format the current time in UTC with millisecond precision and 'Z' suffix
    # cookie = "example_cookie2"
    # country = "US"
    # device = "PC"
    # action = "VIEW"
    # origin = "example_origin"
    # product_id = 123
    # brand_id = "brand456"
    # category_id = "category789"
    price = 999
    key = "1723487940|VIEW"

    value = client.read_key_value(key, set_name="aggregates")
    print(f"Got value: {value}")


    # for set_name in ["aggregates"]:
    #     scan = client.client.scan("mimuw", "aggregates")
    #     keys = []

    #     scan.foreach(lambda x: keys.append(x[0]))
    #     print(f"{set_name}: {len(keys)} rows")

