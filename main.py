from pymongo import MongoClient


def main():
    client = MongoClient("mongodb://lattice-100:27018")
    database_name = "sustaindb"
    db = client[database_name]
    collection_names = db.list_collection_names()
    for collection_name in collection_names:
        print(f"Collection: {collection_name}")


if __name__ == '__main__':
    main()
