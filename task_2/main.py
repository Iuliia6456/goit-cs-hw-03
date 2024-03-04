from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import re
import logging

uri = f"mongodb://localhost:27017/"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

db = client.store

def create_collection():
    try:
        db.create_collection("cats")
        inserted_cats = db.cats.insert_many([
            {"name": "Fluffy", "age": 4, "features": ["fluffy", "soft"]},
            {"name": "Cuddles", "age": 2, "features": ["cuddly", "white"]},
            {"name": "Socks", "age": 5, "features": ["very fast", "plush"]}
        ])
        return inserted_cats
    except Exception as e:
        print(f"An error occurred while creating the collection: {e}")
        raise


def read_all_cats():
    try:
        result = list(db.cats.find({}))
        if len(result) > 0:
            [print (cat) for cat in result]
        else:
            print("\nCats not found\n")
    except Exception as e:
        print(f"An error occurred while reading the collection: {e}")
        raise   

def read_by_name():
    try:
        while True:
            user_input = input("Enter name: ")
            regex_pattern = re.compile(re.escape(user_input), re.IGNORECASE)
            result = list(db.cats.find({"name": {"$regex": regex_pattern}}))
            if len(result) > 0:
                [print (cat) for cat in result]
                break  
            else:
                print("\nNot found, try again.\n")
    except Exception as e:
        print(f"An error occurred while reading the collection: {e}")
        raise

def update_the_age_by_name(name, new_age):
    try:
        regex_pattern = re.compile(re.escape(name), re.IGNORECASE)
        verify_name = db.cats.find_one({"name": {"$regex": regex_pattern}})

        if verify_name is None:
            print(f"\n{name} not found in the database.\n")
        else:
            db.cats.update_one({"name": {"$regex": regex_pattern}}, {"$set": {"age": new_age}})
            print(f"\nThe age of cat with the name {name} has been updated.\n")
            new_result = db.cats.find_one({"name": {"$regex": regex_pattern}})
            print(f"{new_result}\n")
    except Exception as e:
        print(f"An error occurred while updating the collection: {e}")
        raise

def add_the_feature_by_name(name, new_feature):
    try:
        regex_pattern = re.compile(re.escape(name), re.IGNORECASE)
        verify_name = db.cats.find_one({"name": {"$regex": regex_pattern}})

        if verify_name is None:
            print(f"\n{name} not found in the database.\n")
        else:
            db.cats.update_one({"name": {"$regex": regex_pattern}}, {"$push": {"features": new_feature}})
            print(f"\nThe feature {new_feature} for cat with the name {name} has been added.\n")
            new_result = db.cats.find_one({"name": {"$regex": regex_pattern}})
            print(f"{new_result}\n")
    except Exception as e:
        print(f"An error occurred while updating the collection: {e}")
        raise

def delete_by_name(name):
    try:
        regex_pattern = re.compile(re.escape(name), re.IGNORECASE)
        verify_name = db.cats.find_one({"name": {"$regex": regex_pattern}})

        if verify_name is None:
            print(f"\n{name} not found in the database.\n")
        else:
            db.cats.delete_one({"name": {"$regex": regex_pattern}})
            print(f"\nThe cat with the name {name} has been deleted.\n")
    except Exception as e:
        print(f"An error occurred while updating the collection: {e}")
        raise

def delete_all():
    try:
        result = db.cats.delete_many({})
        deleted_count = result.deleted_count
        if deleted_count > 0:
            print("\nAll cats have been deleted.\n")
        else:
            print("\nCats not found.\n")
    except Exception as e:
        print(f"An error occurred while deleting the collection: {e}")
        raise


# create_collection()
# read_all_cats()
#read_by_name()
# update_the_age_by_name(name="socks", new_age=1)
# add_the_feature_by_name(name="socks", new_feature="small")
# delete_by_name(name="socks")
# delete_all()

