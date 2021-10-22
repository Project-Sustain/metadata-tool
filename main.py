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


def exists_in_collection(field, gis_join, collection):
    print(f"Searching for .*{gis_join}.* in field {field}")
    return collection.find_one({field: {"$regex": f".*{gis_join}.*"}}) is not None


def main():
    client = MongoClient("mongodb://lattice-100:27018")
    database_name = "sustaindb"
    db = client[database_name]
    collection_names = db.list_collection_names()
    gis_joins = load_state_gis_joins()

    collections_supported_by_gis_join = {}

    for collection_name in sorted(collection_names):
        print(f"Evaluating collection {collection_name}...")
        collection = db[collection_name]
        first_record = collection.find_one()

        for gis_join in gis_joins:
            collections_supported_by_gis_join[gis_join] = []

            found = False

            if "GISJOIN" in first_record.keys():
                if exists_in_collection("GISJOIN", gis_join, collection):
                    collections_supported_by_gis_join[gis_join].append(collection_name)
                    found = True

            elif "gis_join" in first_record.keys():
                if exists_in_collection("gis_join", gis_join, collection):
                    found = True
                    collections_supported_by_gis_join[gis_join].append(collection_name)

            elif "properties" in first_record.keys():
                if "GISJOIN" in first_record["properties"].keys():
                    if exists_in_collection("properties.GISJOIN", gis_join, collection):
                        found = True
                        collections_supported_by_gis_join[gis_join].append(collection_name)

            print("Found:", found)

    print(collections_supported_by_gis_join)


if __name__ == '__main__':
    main()
