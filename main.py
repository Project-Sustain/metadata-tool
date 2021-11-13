from pymongo import MongoClient
from pprint import pprint
import json


def load_state_gis_joins():
    with open("state_gisjoin.json", "r") as f:
        return json.load(f)


def search_collections_for_gis_joins(gis_joins):
    client = MongoClient("mongodb://lattice-100:27018")
    database_name = "sustaindb"
    db = client[database_name]
    collection_names = db.list_collection_names()

    # Mapping of gis_join -> [ list of collections ]
    collections_supported_by_gis_join = {}

    # Iterate over all collections
    for collection_name in sorted(collection_names):
        if collection_name == "state_gis_join_metadata":
            continue

        print(f"Evaluating collection {collection_name}...")
        collection = db[collection_name]
        found = False

        # Determine if there is a GISJOIN field
        found_in_field = get_gis_join_field(collection)
        if found_in_field:

            # Iterate over all GISJOINs and see if there are any records with that GISJOIN in the collection
            for gis_join in gis_joins:

                # Add empty list if no entry exists yet
                if gis_join not in collections_supported_by_gis_join:
                    collections_supported_by_gis_join[gis_join] = []

                if exists_in_collection(found_in_field, gis_join, collection):
                    collections_supported_by_gis_join[gis_join].append(collection_name)
                    found = True

        else: # No GISJOIN field, check $geoIntersects

            # Iterate over all GISJOINs and see if the state's geometry intersects any documents' geometries
            for gis_join in gis_joins:

                state_geo_document = collection.find({"properties.GISJOIN": gis_join})


def exists_in_collection(field, gis_join, collection):
    print(f"Searching for .*{gis_join}.* in field {field}")
    return collection.find_one({field: {"$regex": f".*{gis_join}.*"}}) is not None


def get_gis_join_field(collection):
    first_record = collection.find_one()
    if first_record:
        if "GISJOIN" in first_record.keys():
            return "GISJOIN"
        elif "gis_join" in first_record.keys():
            return "gis_join"
        elif "properties" in first_record.keys():
            if "GISJOIN" in first_record["properties"].keys():
                return "properties.GISJOIN"
    return None


def create_or_replace_metadata_collection(db, gis_joins, collections_supported_by_gis_join):
    metadata_collection_name = "state_gis_join_metadata"
    metadata_collection = db[metadata_collection_name]
    index = 0
    for gis_join in sorted(gis_joins):
        collections_found_in = collections_supported_by_gis_join[gis_join]
        metadata_collection.replace_one(
            {"_id": index},
            {
                "_id": index,
                "gis_join": gis_join,
                "collections_supported": collections_found_in
            },
            upsert=True
        )
        index += 1


def test():
    client = MongoClient("mongodb://lattice-100:27018")
    database_name = "sustaindb"
    db = client[database_name]
    collection = db["state_geo"]
    test_gis_join = "G480"
    cursor = collection.find({"properties.GISJOIN": test_gis_join})
    for document in cursor:
        pprint(document)


def main():
    #gis_joins = load_state_gis_joins()
    #for gis_join in gis_joins:
    #    print(gis_join["GISJOIN"])
    test()


if __name__ == '__main__':
    main()
