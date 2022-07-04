#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
The input for this script is the sewer data in shape-format (points, lines).
The shapefiles were converted from dwg previously and include a lot of attributes.
The script is tailored to a very specific type of data and expects certain attributes to exist.
Attribute documentation: 
E0101.N01_% : Height of shaft cover
E0101.N02_% : Shaft bottom height (the deepest point)
E0102.N01_% : Height, where the pipe connects to the shaft
E0102.N02_% : Height, where the pipe connects to the shaft (other end)
E0102.N03_% : Length
E0102.N06_% : Diameter (height) in millimeter
E0102.C05_% : Material
E0102.N05_% : Diameter (width) in millimeter
E0102.N08_% : Inclination in parts per thousand
C37_%S      : Comment in free text

Processing steps:
1. Convert the shapefiles into geojson, to prepare them for the database import. Geojson can be imported in Cesium, shapefiles can not.
    The result are two dictionaries, one for points and one for lines.
2. Remove all unneeded points.
3. Add a unique, numerical id.
4. Apply the geoid undulation.
5. For points:
    Combine shaft geometry and attributes
'''

import glob
import os
import json
import shutil
from osgeo import osr, gdal
from math import sqrt
from shapely.geometry import CAP_STYLE, JOIN_STYLE
from shapely.geometry import Point, LineString
from decimal import Decimal

gdal.UseExceptions()

script_path = os.path.dirname(os.path.realpath(__file__))
input_path = os.path.join(script_path, "input")
processing_path = os.path.join(script_path, "processing")
output_path = os.path.join(script_path, "output")
attributeNameMap = {
    "id": "id",
    "AdSPMKey": "AdSPMKey",
    "dwg_handle": "dwg_handle",
    "Layer": "Layer",
    "Color": "Farbe",
    "Linetype": "Linen-Typ",
    "Lineweight": "Linienstärke",
    "Hyperlink": "Hyperlink",
    "Thickness": "Dicke",
    "BlkName": "Blockname",
    "E0101.C01_%": "Schachtnummer",
    "X": "X",
    "Y": "Y",
    "Z": "Z",
    "E0101.N01_%": "Deckelhöhe [m]",
    "E0101.N02_%": "Sohlhöhe [m]",
    "E0102.N03_%": "Länge Aufmaß [m]",
    "E0102.C05_%": "Materialkürzel",
    "E0102.N05_%": "Profilbreite [mm]",
    "E0102.N08_%": "Neigung [‰]",
    "E0102.N01_%": "Ablaufhöhe [m]",
    "E0102.N02_%": "Anlaufhöhe [m]",
    "E0102.N06_%": "Profilhöhe [mm]",
    "C37_%S": "Kommentar",
    "Length": "Length",
    "Area": "Area",
    "Elevation": "Elevation",
}

sourceEPSG = 4647
# Cesium uses the GRS80 ellipsoid as height reference.
# If the input heights are relative to a different ellipsoid / geoid, we have to apply an offset
# Note that using a static offset is only advisable in small areas.
# For germany, the geoid undulation can be calculated by checking the value for multiple points in the target area using.
# http://gibs.bkg.bund.de/geoid/gscomp.php?p=g and building the average.
# This was done for 10 points in Herne, resulting in:
geoid_undulation = 45.43

def main():
    # Delete output and processing directories and create empty ones
    if os.path.exists(processing_path) and os.path.isdir(processing_path):
        shutil.rmtree(processing_path)
    os.mkdir(processing_path)
    if os.path.exists(output_path) and os.path.isdir(output_path):
        shutil.rmtree(output_path)
    os.mkdir(output_path)

    # Get path for each file that should be processed
    files_to_process = glob.glob(os.path.join(input_path, '*.shp'))

    try:
        for file_path in files_to_process:
            # Convert to geojson
            file_name = file_path.split("\\")[-1]
            file_name = replace_last(file_name, ".shp", "")
            input = os.path.join(input_path, file_name + ".shp")
            output = os.path.join(processing_path, file_name + ".geojson")
            print("Converting file " + file_name + ".shp to geoJSON")
            shapefile2geojson(input, output)
            print("Conversion done. Saved result into processing directory.")

        point_geojson = None
        line_geojson = None
        files_to_process = glob.glob(os.path.join(processing_path, '*.geojson'))
        for file_path in files_to_process:
            with open(file_path, 'r') as f:
                json_data = json.load(f)
                geom_type = json_data["features"][0]["geometry"]["type"] # "Point", "LineString"
                if geom_type == "Point":
                    point_geojson = json_data
                elif geom_type == "LineString":
                    line_geojson = json_data
                else:
                    raise Exception("Geometry type was neither 'Point' nor 'LineString'.")

        # Remove unneeded features
        # Some points have the height set to 100, because the real values could not be determined
        print("Removing points with Z value 100")
        point_geojson["features"] = [feature for feature in point_geojson["features"] if int(feature["properties"]["Z"]) != 100]
        point_geojson["features"] = [feature for feature in point_geojson["features"] if feature["properties"]["E0101.N01_%"] != "100.000"] # shaft cover height in _txt layers ( = the text features, that belong to those points)

        # Add an unique id. There are some fields we could use, but we add a new one to be safe
        counter = 1
        for feature in point_geojson["features"]:
            feature["properties"]["id"] = counter
            counter += 1
        counter = 1
        for feature in line_geojson["features"]:
            feature["properties"]["id"] = counter
            counter += 1
        
        # Heights need to be in reference to the GRS80 ellipsoid
        print("Adding height offset (" + str(geoid_undulation) + "m) to geometry.")
        point_geojson = addHeightOffset(point_geojson, "Point", geoid_undulation)
        line_geojson = addHeightOffset(line_geojson, "LineString", geoid_undulation)

        # Get all points that have a Z value.
        shafts = [feature for feature in point_geojson["features"] if int(feature["properties"]["Z"]) > 0]
        shafts = [feature for feature in shafts if feature["properties"]["Layer"] != "Abwasser-Haltungen-Insp-Symbole-DWA-SK"] # TODO ask what that layer means, for now we ignore it.
        # Get all points, where the layer includes "_TXT". These are the text blocks, which contain the information about the shafts / pipes.
        txt_blocks = [feature for feature in point_geojson["features"] if "_TXT" in feature["properties"]["Layer"]]
        # For each point, check if there is a feature (txt_block) with equal coordinates.
        # If yes, that feature belongs to that shaft.
        for idx, shaft in enumerate(shafts):
            props = shaft["properties"]
            # initial values. gets overwritten if attributes are found
            props["Color"] = "150,150,150"
            point_x = float(props["X"])
            point_y = float(props["Y"])
            for idx, txt_block in enumerate(txt_blocks):
                txt_props = txt_block["properties"]
                txt_x = float(txt_props["X"])
                txt_y = float(txt_props["Y"])
                # This works for some points, but not for all. Mostly for shafts
                if(point_x == txt_x and point_y == txt_y):
                    props["Color"] = txt_props["Color"]
                    props["Linetype"] = txt_props["Linetype"]
                    props["Lineweight"] = txt_props["Lineweight"]
                    props["Hyperlink"] = txt_props["Hyperlink"]
                    props["Thickness"] = txt_props["Thickness"]
                    props["E0101.N01_%"] = float( txt_props["E0101.N01_%"] ) if txt_props["E0101.N01_%"] is not None else txt_props["E0101.N01_%"]
                    props["E0101.N02_%"] = float( txt_props["E0101.N02_%"] ) if txt_props["E0101.N02_%"] is not None else txt_props["E0101.N02_%"]
                    props["E0101.C01_%"] = txt_props["E0101.C01_%"]
                    props["E0102.C05_%"] = txt_props["E0102.C05_%"]
                    props["C37_%S"] = txt_props["C37_%S"]
                    props["Elevation"] = txt_props["Elevation"]

        #shafts = [feature for feature in shafts if feature["properties"]["E0101.N01_%"] and feature["properties"]["E0101.N02_%"]]
        point_geojson["features"] = shafts

        # Now we have to do the same for the pipes.
        # The geometries are in one file, the information (as point objects) in the other
        pipes = line_geojson["features"]
        epsilon = 0.1 # length plus minus interval
        buffer_distance = 1 # 2D buffer, 1 meter
        for idx, pipe in enumerate(pipes):
            props = pipe["properties"]
            # Initial values. Gets overwritten if attributes are found
            props["Color"] = "150,150,150"
            props["E0102.N05_%"] = 300
            # buffer line geom and round length to
            length2D = getLinestringLength2D(pipe["geometry"]["coordinates"])
            length2D = round(length2D, 3)
            buffer = LineString(pipe["geometry"]["coordinates"]).buffer(buffer_distance, cap_style=CAP_STYLE.round, join_style=JOIN_STYLE.round) 
            
            for idx, txt_block in enumerate(txt_blocks):
                txt_props = txt_block["properties"]
                x = txt_props["X"]
                y = txt_props["Y"]
                txt_coord = Point(float(x), float(y))
                txt_length2D = txt_props["E0102.N03_%"]
                if txt_length2D is None:
                    continue
                txt_length2D = round(float(txt_length2D), 2)
                if length2D - epsilon <= txt_length2D and txt_length2D <= length2D + epsilon and buffer.contains(txt_coord):
                    props["Color"] = txt_props["Color"]
                    props["Linetype"] = txt_props["Linetype"]
                    props["Lineweight"] = txt_props["Lineweight"]
                    props["Hyperlink"] = txt_props["Hyperlink"]
                    props["Thickness"] = txt_props["Thickness"]
                    props["E0102.N01_%"] = float( txt_props["E0102.N01_%"] ) if txt_props["E0102.N01_%"] is not None else txt_props["E0102.N01_%"]
                    props["E0102.N02_%"] = float( txt_props["E0102.N02_%"] ) if txt_props["E0102.N02_%"] is not None else txt_props["E0102.N02_%"]
                    props["E0102.N03_%"] = float( txt_props["E0102.N03_%"] ) if txt_props["E0102.N03_%"] is not None else txt_props["E0102.N03_%"]
                    props["E0102.N05_%"] = float( txt_props["E0102.N05_%"] ) if txt_props["E0102.N05_%"] is not None else txt_props["E0102.N05_%"]
                    props["E0102.N06_%"] = txt_props["E0102.N06_%"]
                    props["E0102.N08_%"] = float( txt_props["E0102.N08_%"] ) if txt_props["E0102.N08_%"] is not None else txt_props["E0102.N08_%"]
                    props["E0101.C01_%"] = txt_props["E0101.C01_%"]
                    props["E0102.C05_%"] = txt_props["E0102.C05_%"]
                    props["C37_%S"] = txt_props["C37_%S"]
                    props["Elevation"] = txt_props["Elevation"]
                    break

        line_geojson["features"] = pipes
        # Has to be done after calculating length and buffer
        print("Reprojecting to EPSG:4326")
        with open(os.path.join(processing_path, "shafts.geojson"), 'w+') as f:
            json.dump(point_geojson, f, separators=(',', ':'))
        with open(os.path.join(processing_path, "pipes.geojson"), 'w+') as f:
            json.dump(line_geojson, f, separators=(',', ':'))

        reprojectToWGS84(os.path.join(processing_path, "shafts.geojson"), os.path.join(processing_path, "shaftsWgs84.geojson"), sourceEPSG)
        reprojectToWGS84(os.path.join(processing_path, "pipes.geojson"), os.path.join(processing_path, "pipesWgs84.geojson"), sourceEPSG)
        
        with open(os.path.join(processing_path, "shaftsWgs84.geojson"), 'r') as f:
            point_geojson = json.load(f)
            shafts = point_geojson["features"]
        with open(os.path.join(processing_path, "pipesWgs84.geojson"), 'r') as f:
            line_geojson = json.load(f)
            pipes = line_geojson["features"]

        # Rename properties
        print("Renaming properties")
        for idx, shaft in enumerate(shafts):
            keys = list(shaft["properties"].keys()).copy()
            for key in keys:
                if key in attributeNameMap:
                    renameProperty(shaft["properties"], key, attributeNameMap[key])
        for idx, pipe in enumerate(pipes):
            keys = list(pipe["properties"].keys()).copy()
            for key in keys:
                if key in attributeNameMap:
                    renameProperty(pipe["properties"], key, attributeNameMap[key])
        
        # Save to disk
        output = wrapInFeatureCollection(shafts, "shafts.geojson")
        with open(os.path.join(output_path, "shafts.geojson"), 'w+', encoding="utf8") as f:
            # write minified. replace separators with indent=2 --> unminified
            json.dump(output, f, separators=(',', ':'))

        output = wrapInFeatureCollection(pipes, "pipes.geojson")
        with open(os.path.join(output_path, "pipes.geojson"), 'w+', encoding="utf8") as f:
            # write minified. replace separators with indent=2 --> unminified
            json.dump(output, f, separators=(',', ':'))
        
        print("Script done.")
    except Exception as e:
        raise(e)
    finally:
        print("Cleaning up processing directory")
        shutil.rmtree(processing_path)
        os.mkdir(processing_path)


def shapefile2geojson(infile, outfile):
    '''Translates a shapefile to GEOJSON'''
    options = gdal.VectorTranslateOptions(format="GeoJSON")
    gdal.VectorTranslate(outfile, infile, options=options)


# Only call on geoJson files with this implementation
def reprojectToWGS84(infile, outfile, sourceEPSG):
    options = gdal.VectorTranslateOptions(
        format="GeoJSON",
        srcSRS="EPSG:" + str(sourceEPSG),
        dstSRS="EPSG:4326"
    )
    gdal.VectorTranslate(outfile, infile, options=options)


# Adds a height offset to the geojson files.
# This changes the geometry height only, not the attribute values.
def addHeightOffset(json_data, geom_type, geoid_undulation):
    for feature in json_data["features"]:
        coords = feature["geometry"]["coordinates"]
        if geom_type == "Point":
            coords[2] += geoid_undulation # lon, lat, height
        if geom_type == "LineString":
            for idx, point in enumerate(coords):
                coords[idx][2] += geoid_undulation
        # Has only one feature, which is the area bbox.
        if geom_type == "Polygon":
            for idx, point in enumerate(coords[0]):
                coords[0][idx][2] += geoid_undulation
    
    return json_data


# writes a Geojson file of geometry type LineString
def wrapInFeatureCollection(features, filename):
    # modify output path
    result = dict()
    result["type"] = "FeatureCollection"
    result["name"] = filename.replace(".geojson", "")
    result["crs"] = dict()
    crs = result["crs"]
    crs["type"] = "name"
    crs["properties"] = dict(name="urn:ogc:def:crs:OGC:1.3:CRS84")
    result["features"] = []

    for feature in features:
        result["features"].append( feature )

    return result
    

def replace_last(str, old, new):
    last_char_index = str.rfind(old)
    new_string = str[:last_char_index] + new + str[last_char_index+len(old):]
    return new_string

def getLinestringLength2D(coords):
    result = 0
    for i in range(1, len(coords)):
        prev_point = coords[i-1]
        point = coords[i]
        segment_length = sqrt(
            (prev_point[0] - point[0]) * (prev_point[0] - point[0]) +
            (prev_point[1] - point[1]) * (prev_point[1] - point[1]))
        result += segment_length
    return result

def getLinestringLength3D(coords):
    result = 0
    for i in range(1, len(coords)):
        prev_point = coords[i-1]
        point = coords[i]
        segment_length = sqrt(
            (prev_point[0] - point[0]) * (prev_point[0] - point[0]) +
            (prev_point[1] - point[1]) * (prev_point[1] - point[1]) +
            (prev_point[2] - point[2]) * (prev_point[2] - point[2]))
        result += segment_length
    return result

# https://stackoverflow.com/a/25310860/18450475
def renameProperty(obj, old_name, new_name):
        obj[new_name] = obj.pop(old_name)

# Call main method now that all functions are known
main()