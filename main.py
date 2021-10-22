from pymongo import MongoClient


def load_state_gis_joins():
    gis_joins = []
    with open("state_gisjoins.txt", "r") as f:
        lines = f.readlines()
        for line in lines:
            if line.strip():
                gis_joins.append(line)

    return gis_joins


def main():
    client = MongoClient("mongodb://lattice-100:27018")
    database_name = "sustaindb"
    db = client[database_name]
    collection_names = db.list_collection_names()
    gis_joins = load_state_gis_joins()
    for collection_name in collection_names:
        print(f"Collection: {collection_name}")

    print(gis_joins)


if __name__ == '__main__':
    main()
