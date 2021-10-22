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


def get_gis_join_field(collection):
    first_record = collection.find_one()
    if "GISJOIN" in first_record.keys():
        return "GISJOIN"
    elif "gis_join" in first_record.keys():
        return "gis_join"
    elif "properties" in first_record.keys():
        if "GISJOIN" in first_record["properties"].keys():
            return "properties.GISJOIN"
    else:
        return None


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
        found = False

        for gis_join in gis_joins:
            # Add empty list if no entry exists yet
            if gis_join not in collections_supported_by_gis_join:
                collections_supported_by_gis_join[gis_join] = []

            found_in_field = get_gis_join_field(collection)
            if found_in_field:
                if exists_in_collection(found_in_field, gis_join, collection):
                    collections_supported_by_gis_join[gis_join].append(collection_name)
                    found = True

            print("Found:", found)

    print(collections_supported_by_gis_join)

    metadata_collection_name = "state_gis_join_metadata"
    metadata_collection = db[metadata_collection_name]
    index = 0
    for gis_join in sorted(gis_joins):
        collections_found_in = collections_supported_by_gis_join[gis_join]
        metadata_collection.insert_one({
            "_id": index,
            "gis_join": gis_join,
            "collections_supported": collections_found_in
        })


if __name__ == '__main__':
    main()
