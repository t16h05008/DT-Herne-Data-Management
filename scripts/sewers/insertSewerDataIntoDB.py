#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Bulk import GeoJSON file into MongoDB

# This script uses the filename of the input files (in subfolder ./output) to determine which collection to insert them in.
# The filenames should include "point", "line" or "polygon". For each geometry type a separate collection is created / updated.
# The naming scheme is [config.collection].points (or .line / .polygon), eg. sewerData.point, sewerData.line and sewerData.polygon
# Also, collections are dropped and recreated for now, so only use this script to import a complete dataset

import os
import json
import glob
from pathlib import Path
from datetime import datetime
from pymongo import MongoClient, GEOSPHERE, InsertOne
from pymongo.errors import (PyMongoError, BulkWriteError)

script_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(script_path, "config")
config_filename = "updateMongoDbSewersDatabase.config.json"
input_path = os.path.join(script_path, "output") # take the output of the preparation script as input

def main():
    config = readConfig()
    uri = 'mongodb://' + config["server"] + ':' + str(config["port"]) +'/' # TODO add credentials
    client = MongoClient(uri)
    db = client[config["database"]]
    
     # get path for each file that should be processed
    files_to_process = glob.glob(os.path.join(input_path, '*.geojson'))
    for idx, file_path in enumerate(files_to_process):
        filename = file_path.split("\\")[-1]
        collection_suffix = ""
        if "point.geojson" in filename:
            collection_suffix = "shafts"
        elif "point_as_lines.geojson" in filename:
            collection_suffix = "shaftsAsLines"
        elif "line.geojson" in filename:
            collection_suffix = "pipes"
        # Store the bbox, too, but it's likely never queried...
        elif "polygon.geojson" in filename:
            collection_suffix = "bbox"
        else:
            raise("Filename must contain 'point.geojson', 'point_as_lines.geojson', 'line.geojson' or 'polygon.geojson' (to determine the collection to insert into)")

        collection_name = config["collection"] + "." + collection_suffix
        # drop collection id it existed
        if(collection_name in db.list_collection_names()):
            collection = db[collection_name].drop()
        
        collection = db[collection_name]

        geojson = None
        with open(file_path,'r') as f:
            geojson = json.loads(f.read())

        # create 2dsphere index and initialize unordered bulk insert
        # 2d index doesn't work for strivtly vertical lines, so we don't use it there
        # For now, we return all data anyway so there is no use for an index (yet)
        # Another workaround would be to slightly alter one of the coordinates, like in the 15th decimal place or something
        if(collection_name != config["collection"] + ".shaftsAsLines"):
            collection.create_index([("geometry", GEOSPHERE)])
    
        bulk_arr = []
        for feature in geojson['features']:
            bulk_arr.append( InsertOne(feature) )
        
        # execute bulk operation to the DB
        try:
            result = collection.bulk_write(bulk_arr)
            print("Number of Features successfully inserted:", result.inserted_count)
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