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
config_filename = "insertSewerDataIntoDB.config.json"
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
        if "point.geojson" in filename:
            collection_suffix = "shafts"
        elif "point.bboxInfo.json" in filename:
            collection_suffix = "shafts.bboxInfo"
            is_bbox_file = True
        elif "point_as_lines.geojson" in filename:
            collection_suffix = "shaftsAsLines"
        elif "point_as_lines.bboxInfo.json" in filename:
            collection_suffix = "shaftsAsLines.bboxInfo"
            is_bbox_file = True
        elif "line.geojson" in filename:
            collection_suffix = "pipes"
        elif "line.bboxInfo.json" in filename:
            collection_suffix = "pipes.bboxInfo"
            is_bbox_file = True
        # Store the bbox, too, but it's likely never queried...
        elif "polygon.geojson" in filename:
            collection_suffix = "polygon"
        elif "polygon.bboxInfo.json" in filename:
            collection_suffix = "polygon.bboxInfo"
            is_bbox_file = True
        else:
            raise Exception("Filename must contain \
                'point.geojson', \
                'point.bboxInfo.json', \
                'point_as_lines.geojson', \
                'point_as_lines.bboxInfo.json', \
                'line.geojson' \
                'line.bboxInfo.json', \
                'polygon.geojson' or \
                'polygon.bboxInfo.json' (to determine the collection to insert into)")

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