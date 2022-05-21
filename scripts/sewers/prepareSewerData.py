'''
The input for this script is the sewer data in shape-format (points, lines).
The shapefiles were converted from dwg previously and include a lot of attributes.
This script converts the shapefiles into geojson, to prepare them for the database import.
Geojson can be imported in Cesium, shapefiles can not.
Also, some further data processing is done (see below).

This script is tailored to a very specific type of data and expects certain attributes to exist.
'''

import glob
import os
import json
import copy
from osgeo import osr, gdal

gdal.UseExceptions()

script_path = os.path.dirname(os.path.realpath(__file__))
input_path = os.path.join(script_path, "input")
output_path = os.path.join(script_path, "output")
sourceEPSG = 4647
# Cesium uses the GRS80 ellipsoid as height reference.
# If the input heights are relative to a different ellipsoid / geoid, we have to apply an offset
# Note that using a static offset is only advisable in small areas.
# For germany, the geoid undulation can be calculated by checking the value for multiple points in the target area using.
# http://gibs.bkg.bund.de/geoid/gscomp.php?p=g and building the average.
# This was done for 10 points in Herne, resulting in:
geoid_undulation = 45.43

def main():
    # Remove files in output dir
    files_to_delete = glob.glob(os.path.join(output_path, "*.geojson"))
    for f in files_to_delete:
        os.remove(f)
    # Get path for each file that should be processed
    files_to_process = glob.glob(os.path.join(input_path, '*.shp'))

    for file_path in files_to_process:
        # Convert to geojson first
        filename = file_path.split("\\")[-1]
        filename = replace_last(filename, ".shp", "")
        input = os.path.join(input_path, filename + ".shp")
        output = os.path.join(output_path, filename + ".geojson")
        print("Converting file " + filename + ".shp to geoJson (reprojecting to EPSG:4326)")
        shapefile2geojson(input, output, sourceEPSG)
        print("Conversion done.")
        print("Doing some further processing...")
       

        # Now that we work with geojson we have to do some more processing.
        # For now, we keep all sewer-types in one file. They can be styled differently in the client,
        # but not toggled separately.
        # The processing steps are:
        # 1. Add the geoid undulation
        # 2. Add a unique id
        # 3. For points:
        #       Derive LineString geometries to visualize the shaft depths
        # 4. Calculate bounding boxes for each feature and store them in a separate geojson file
        
        json_data = None
        with open(output, 'r') as f:
            json_data = json.load(f)
        
        # (Step 1) Heights need to be adjusted to account for the different height reference
        json_data = addHeightOffset(json_data)
        # (Step 2) Add an unique id. There are some fields we could use, but we add a new one to be safe
        counter = 1
        for feature in json_data['features']:
            feature["properties"]["id"] = counter
            counter += 1
        
        # (Step 3, only for point layer)
        # If we opened the point layer (shafts), we have a special case.
        # There are actually two point objects for many shafts.
        # One has the geometry, the other one (_TXT layer) has the attributes
        shafts_as_linestring_geojson = None
        shafts_as_linestring_path = ""
        if json_data["features"][0]["geometry"]["type"] == "Point":
            # Remove points ones with height = 100
            json_data["features"] = [feature for feature in json_data["features"] if feature["properties"]["Z"] < 100]
            print("Deriving new LineString Geojson from sewer shafts")
            # This method is somewhat tailored to the data at hand
            shafts_as_linestring_path = replace_last(output, ".geojson", "")
            shafts_as_linestring_path += "_as_lines.geojson"
            shafts_as_linestring_filename = shafts_as_linestring_path.split("\\")[-1] # gets included in the geojson, bit we don't save anything to disk yet
            shafts_as_linestring_geojson = deriveLineStringGeometries(json_data, shafts_as_linestring_filename)
            # Now remove all point that have no Z-Coordinate to reduce entities to show
            json_data["features"] = [feature for feature in json_data["features"] if feature["properties"]["Z"] > 0]

        
        # (Step 4) Calculate bounding box references and save them as separate files
        bboxInfo = calculateBoundingBoxInfo(json_data)
        bbox_output_path = replace_last(output, ".geojson", ".bboxInfo.json")
        with open(bbox_output_path, 'w+') as f:
            # write minified. replace separators with indent=2 --> unminified separators=(',', ':')
            json.dump(bboxInfo, f, separators=(',', ':'))
            pass

        if shafts_as_linestring_geojson:
            bboxInfo = calculateBoundingBoxInfo(shafts_as_linestring_geojson)
            bbox_output_path = replace_last(shafts_as_linestring_path, ".geojson", ".bboxInfo.json")
            with open(bbox_output_path, 'w+') as f:
                # write minified. replace separators with indent=2 --> unminified 
                json.dump(bboxInfo, f, separators=(',', ':'))
                pass
        
        # save to disk
        with open(output, 'w+') as f:
            # write minified. replace separators with indent=2 --> unminified
            json.dump(json_data, f, separators=(',', ':'))
        

        if shafts_as_linestring_geojson: 
            with open(shafts_as_linestring_path, 'w+') as f:
                # write minified. replace separators with indent=2 --> unminified
                json.dump(shafts_as_linestring_geojson, f, separators=(',', ':')) 

    print("Script done.")


def shapefile2geojson(infile, outfile, sourceEPSG):
    '''Translate a shapefile to GEOJSON.'''
    # Hardcoded for now
    options = gdal.VectorTranslateOptions(
        format="GeoJSON",
        srcSRS="EPSG:" + str(sourceEPSG),
        dstSRS="EPSG:4326")
    gdal.VectorTranslate(outfile, infile, options=options)


def addHeightOffset(json_data):
    print("Adding height offset (" + str(geoid_undulation) + "m)")
    for feature in json_data['features']:
        geom = feature['geometry']
        coords = geom["coordinates"]
        if geom["type"] == "Point":
            coords[2] += geoid_undulation
        if geom["type"] == "LineString":
            for idx, point in enumerate(coords):
                coords[idx][2] += geoid_undulation
        # Polygon doesn't get imported into db, but whatever
        if geom["type"] == "Polygon":
            for idx, point in enumerate(coords[0]):
                coords[0][idx][2] += geoid_undulation
    
    return json_data


# writes a Geojson file of geometry type LineString
def createNewGeojsonFile(features, filename):
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
        result["features"].append( createGeojsonLineStringFeature(feature) )

    return result
    

def createGeojsonLineStringFeature(feature):
    result = dict()
    result["type"] = "Feature"
    result["properties"] = dict()
    result["geometry"] = dict()
    props = result["properties"]
    for k, v in feature["properties"].items():
        props[k] = v

    geom = result["geometry"]
    geom["type"] = "LineString"
    geom["coordinates"] = []
    coords = geom["coordinates"]

    coord_low = [
        feature["geometry"]["coordinates"][0], # lon
        feature["geometry"]["coordinates"][1], # lat
        feature["properties"]["E0101.N02_%"] + geoid_undulation # lower height value, also apply offset here since we took the value from an attribute field
    ]
    coord_high = [
        feature["geometry"]["coordinates"][0], # lon
        feature["geometry"]["coordinates"][1]+0.000000000001, # lat Add a slight offset because vertical lines lead to an error in cesium
        feature["properties"]["E0101.N01_%"] + geoid_undulation # higher height value, also apply offset here since we took the value from an attribute field
    ]
    coords.append(coord_low) # line start
    coords.append(coord_high) # line end

    return result



def replace_last(str, old, new):
    last_char_index = str.rfind(old)
    new_string = str[:last_char_index] + new + str[last_char_index+len(old):]
    return new_string


def deriveLineStringGeometries(json_data, filename):
     # Get all points that have a z value
    # "Z" is not affected by previous height offset
    features = copy.deepcopy(json_data)["features"]
    filtered = [feature for feature in features if feature["properties"]["Z"] > 0]
    
    # For each point, check if there is another object with the same XY-coords
    # and the same height value in the field 'E0101.N01_%'
    for idx, feature_with_z in enumerate(filtered):
        z_value = feature_with_z["properties"]["Z"]
        for feature in features:
            e0101N01_value = feature["properties"]["E0101.N01_%"]
            
            if(e0101N01_value != None):
                e0101N01_value = float(e0101N01_value)
            else:
                continue

            if feature_with_z["properties"]["X"] == feature["properties"]["X"] and \
              feature_with_z["properties"]["Y"] == feature["properties"]["Y"] and \
              z_value == e0101N01_value:
                
                
                # If yes, add information of that object to the filtered list
                feature_with_z["properties"]["E0101.N01_%"] = e0101N01_value
                e0101N02_value = feature["properties"]["E0101.N02_%"]
                e0101N02_value = float(e0101N02_value)
                feature_with_z["properties"]["E0101.N02_%"] = e0101N02_value

        filtered[idx] = feature_with_z # apply changes
        
    # Not all features with Z values have a _TXT counterpart, so E0101.N01_% and E0101.N02_% might still be None
    # If that's the case remove these features from filtered
    filtered = [feature for feature in filtered if feature["properties"]["E0101.N01_%"] is not None and  feature["properties"]["E0101.N02_%"] is not None]

    # The stored objects have a height value in 'E0101.N01_%' and a lower height value in
    # 'E0101.N02_%'. This allows us to calculate the shaft depth.
    # Create a new geojson file (geometry: LineString) for these objects, so we can visualize shaft depths.
    
    linestring_geojson = createNewGeojsonFile(filtered, filename)
    return linestring_geojson


# Calculates the bounding boy for each feature and stores it in a separate dict
# The original feature is referenced by id
def calculateBoundingBoxInfo(json):
    result = dict()
    result["bboxReferences"] = dict()
    bboxReferences = result["bboxReferences"]

    for feature in json["features"]:
        id = feature["properties"]["id"]
        bboxReferences[id] = calculateBbox(feature["geometry"])

    return result

# geometry is a geojson feature geometry with properties "type" and "coordinates"
# Couldn't find any good library for this (that an handle 3d geometries), so we have to do it manually...
# The bbox is defined by two points for minimal file size in the db
def calculateBbox(geometry):
    result = dict()
    # check type
    type = geometry["type"]
    minX, maxX, minY, maxY, minZ, maxZ = None,None,None,None,None,None
    # A bbox for points makes no sense, really.
    # But we store it anyway, so we can use a consistent approach in the client
    if(type == "Point"):
        minX = geometry["coordinates"][0]
        maxX = geometry["coordinates"][0]
        minY = geometry["coordinates"][1]
        maxY = geometry["coordinates"][1]
        minZ = geometry["coordinates"][2]
        maxZ = geometry["coordinates"][2]

    if(type == "LineString"):
        for coords in geometry["coordinates"]:
            if minX is None: minX = coords[0]
            if minY is None: minY = coords[1]
            if minZ is None: minZ = coords[2]
            if maxX is None: maxX = coords[0]
            if maxY is None: maxY = coords[1]
            if maxZ is None: maxZ = coords[2]
            minX = minX if coords[0] >= minX else coords[0]
            minY = minY if coords[1] >= minY else coords[1]
            minZ = minZ if coords[2] >= minZ else coords[2]
            maxX = maxX if coords[0] <= maxX else coords[0]
            maxY = maxY if coords[1] <= maxY else coords[1]
            maxZ = maxZ if coords[2] <= maxZ else coords[2]
    if(type == "Polygon"):
        for poly in geometry["coordinates"]:
            for coords in poly:
                if minX is None: minX = coords[0]
                if minY is None: minY = coords[1]
                if minZ is None: minZ = coords[2]
                if maxX is None: maxX = coords[0]
                if maxY is None: maxY = coords[1]
                if maxZ is None: maxZ = coords[2]
                minX = minX if coords[0] >= minX else coords[0]
                minY = minY if coords[1] >= minY else coords[1]
                minZ = minZ if coords[2] >= minZ else coords[2]
                maxX = maxX if coords[0] <= maxX else coords[0]
                maxY = maxY if coords[1] <= maxY else coords[1]
                maxZ = maxZ if coords[2] <= maxZ else coords[2]

    result["pMin"] = [minX, minY, minZ]
    result["pMax"] = [maxX, maxY, maxZ]
    return result

# Call main method now that all functions are known
main()