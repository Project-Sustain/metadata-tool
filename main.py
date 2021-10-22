from pymongo import MongoClient
from pprint import pprint


def load_state_gis_joins():
    gis_joins = []
    with open("state_gisjoins.txt", "r") as f:
        lines = f.readlines()
        for line in lines:
            if line.strip():
                gis_joins.append(line.strip())

    return gis_joins


def main():
    client = MongoClient("mongodb://lattice-100:27018")
    database_name = "sustaindb"
    db = client[database_name]
    collection_names = db.list_collection_names()
    gis_joins = load_state_gis_joins()

    for gis_join in gis_joins:
        for collection_name in collection_names:
            collection = db[collection_name]
            first_record = collection.find_one()

            pprint(first_record.keys())
        break


if __name__ == '__main__':
    main()
