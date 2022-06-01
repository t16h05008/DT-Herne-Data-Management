#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Bulk import GeoJSON file into MongoDB

# This script inserts the files in ./output into the database. For each geometry type a separate collection is created / updated.
# The naming scheme is [config.collection].points (or .line), eg. sewerData.point, sewerData.line.
# Also, collections are dropped and recreated for now, so only use this script to import a complete dataset

import os
import json
import glob
from pathlib import Path
from pymongo import MongoClient, GEOSPHERE, InsertOne
from pymongo.errors import (PyMongoError, BulkWriteError)

script_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(script_path, "config")
config_filename = "insertSewersIntoDB.config.json"
input_path = os.path.join(script_path, "output") # take the output of the preparation script as input

def main():
    config = readConfig()
    uri = 'mongodb://' + config["server"] + ':' + str(config["port"]) +'/' # TODO add credentials
    client = MongoClient(uri)
    db = client[config["database"]]
    
     # get path for each file that should be processed
    files_to_process = glob.glob(os.path.join(input_path, '*.geojson'))
    files_to_process.extend(glob.glob(os.path.join(input_path, '*.json')))

    for file_path in files_to_process:
        filename = file_path.split("\\")[-1]
        collection_suffix = ""
        is_bbox_file = False
        if filename == "shafts.geojson" :
            collection_suffix = "shafts"
        elif filename == "pipes.geojson":
            collection_suffix = "pipes"
        elif filename == "pipes.bboxInfo.json":
            collection_suffix = "pipes.bboxInfo"
            is_bbox_file = True
        else:
            raise Exception("Filename must be \
                'shafts.geojson', \
                'pipes.geojson', or \
                'pipes.bboxInfo.json' (to determine the collection to insert into)")

        collection_name = config["collection"] + "." + collection_suffix
        # drop collection id it existed
        if(collection_name in db.list_collection_names()):
            collection = db[collection_name].drop()
        
        collection = db[collection_name]

        # Do different things depending on if we have a bboxInfo file or the real geojson
        if(is_bbox_file):
            # Insert as normal document
            file_content = None
            with open(file_path,'r') as f:
                file_content = json.loads(f.read())
                # Add a reference to the original collection
                file_content["collectionName"] = collection.name.replace(".bboxInfo", "")
                # Insert into db
                result = collection.insert_one(file_content)
                print("Inserted bbox info for", file_content["collectionName"])
        else:
            # insert as geojson
            geojson = None
            with open(file_path,'r') as f:
                geojson = json.loads(f.read())

            # create 2dsphere index
                collection.create_index([("geometry", GEOSPHERE)])

            bulk_arr = []
            for feature in geojson['features']:
                bulk_arr.append( InsertOne(feature) )

            # execute bulk operation to the DB
            try:
                result = collection.bulk_write(bulk_arr)
                print("Collection:", collection.name, ": Number of Features successfully inserted:", result.inserted_count)
            except BulkWriteError as bwe:
                nInserted = bwe.details["nInserted"]
                errMsg = bwe.details["writeErrors"]
                print("Errors encountered inserting features")
                print("Number of Features successfully inserted:", nInserted)
                print("The following errors were found:")
                for item in errMsg:
                    print("Index of feature:", item["index"])
                    print("Error code:", item["code"])
                    print("Message (truncated due to data length):", item["errmsg"][0:120], "...")


def readConfig():
    config_file = Path(os.path.join(config_path, config_filename))
    if config_file.is_file():
        # file exists
        with open(config_file) as f:
            return json.load(f)
    else:
        raise("No config file found.")

main()