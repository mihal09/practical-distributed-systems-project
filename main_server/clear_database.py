from connections import AerospikeClient


if __name__ == "__main__":
    client = AerospikeClient()

    set_names = ["tags", "aggregates"]

    for set_name in set_names:
        client.clear_setname(set_name=set_name)

    for set_name in set_names:
        scan = client.client.scan("mimuw", set_name)
        keys = []

        scan.foreach(lambda x: keys.append(x[0]))
        print(f"{set_name}: {len(keys)} rows")