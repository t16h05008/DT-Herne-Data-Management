from concurrent.futures import process
import glob
import os
from osgeo import osr, gdal
import numpy as np
import gzip
import shutil
from types import SimpleNamespace
import subprocess
gdal.UseExceptions()

# Input data should be placed in ./input

script_path = os.path.dirname(os.path.realpath(__file__))
input_path = os.path.join(script_path, "input")
processing_path = os.path.join(script_path, "processing")
output_path = os.path.join(script_path, "output")
# Cesium uses the GRS80 ellipsoid as height reference.
# If the input heights are relative to a different ellipsoid / geoid, we have to apply an offset
# Note that using a static offset is only advisable in small areas.
# For germany, the geoid undulation can be calculated by checking the value for multiple points in the target area using.
# http://gibs.bkg.bund.de/geoid/gscomp.php?p=g and building the average.
# This was done for 10 points in Herne, resulting in:
geoid_undulation = 45.43

config = SimpleNamespace()
# Enable or disable these processing steps
# If this is false the unzipped data should be placed under ./processing/0_xyz
config.unzip = True 
# This is usefull for debugging to not run the whole script each time, since the conversion takes the longest.
# If this is false the tiff files should be placed under ./processing/1_tiff
config.convertToTiff = True


# The target resolution of the DEM
# The data is provided in a 1m x 1m grid
# If multiple values are supplied the files are resampled to each of the resolutions, creating multiple DEMs.
config.demResolutions = [1, 10, 25, 50]

srs = osr.SpatialReference()
srs.ImportFromEPSG(4326)
epsg4326 = srs.ExportToWkt()
srs.ImportFromEPSG(25832)
epsg25832 = srs.ExportToWkt()

# Disk space usage could be optimized by deleting temporary folders earlier...

def main():
    try:
        if not os.path.exists(processing_path):
            print("Creating subdir ./processing")
            os.mkdir(processing_path)

        if not os.path.exists(output_path):
            print("Creating subdir ./output")
            os.mkdir(output_path)
        
        # get path for each file that should be processed
        files_to_process = glob.glob(os.path.join(input_path, '*.xyz.gz'))
        # iterate files
        number_of_tiles = len(files_to_process)
        for idx, data_path in enumerate(files_to_process):
            print("Processing file " + str(idx+1) + " of " + str(number_of_tiles+1)) 
            filename = data_path.split("\\")[-1]
            path = data_path # full path
            old_path = data_path # stores the reference to the old file path when gdal creates new files
            # Unzip in processing folder
            if(config.unzip):
                filename = replace_last(filename, ".gz", "")
                path = os.path.abspath(os.path.join(path, "../../processing/0_xyz", filename))

                if not os.path.exists(os.path.join(processing_path, "0_xyz")):
                    print("Creating subdir ./processing/0_xyz")
                    os.mkdir(os.path.join(processing_path, "0_xyz"))

                print("Unzipping files to ./processing/0_xyz")
                unzip(data_path, path)
            else:
                # Set the paths in case this step is skipped
                filename = replace_last(filename, ".gz", "")
                path = os.path.join(processing_path, "0_xyz", filename)

            if(config.convertToTiff):
                if not os.path.exists(os.path.join(processing_path, "1_tiff")):
                    print("Creating subdir ./processing/1_tiff")
                    os.mkdir(os.path.join(processing_path, "1_tiff"))

                old_path = path
                filename = replace_last(filename, "xyz", "tiff")
                path = os.path.join(processing_path, "1_tiff", filename)

                print("Converting file to .tiff")
                xyzToTiff(old_path, path)
                # apply geoid undulation
                if not os.path.exists(os.path.join(processing_path, "1_tiff_offset")):
                    print("Creating subdir ./processing/1_tiff_offset")
                    os.mkdir(os.path.join(processing_path, "1_tiff_offset"))

                old_path = path
                path = os.path.join(processing_path, "1_tiff_offset", filename)
                command = r'"C:\Program Files\QGIS 3.16.16\OSGeo4W.bat" ' # TODO This will not work in most cases
                command += "gdal_calc.py -A \"" + old_path + "\" --outfile=\"" + path + "\" --calc=A+" + str(geoid_undulation)
                print(command)
                subprocess.run(command, shell=True)
                print("Converting done")
            else:
                # Set the paths in case this step is skipped
                filename = replace_last(filename, ".xyz", ".tiff")
                path = os.path.join(processing_path, "1_tiff", filename)


            print("Reprojecting to WGS84")
            if not os.path.exists(os.path.join(processing_path, "2_WGS84")):
                    print("Creating subdir ./processing/2_WGS84")
                    os.mkdir(os.path.join(processing_path, "2_WGS84"))
            old_path = path
            path = os.path.join(processing_path, "2_WGS84", filename)
            print(old_path)
            print(path)
            reprojectToWGS84(old_path, path)
            print("Reprojection done")

            print("Replacing noData values with mean")
            noDataToMean(path)
            print("Replacing done")

            print("Resampling file")
            if not os.path.exists(os.path.join(processing_path, "4_resampled")):
                    print("Creating subdir ./processing/4_resampled")
                    os.mkdir(os.path.join(processing_path, "4_resampled"))
            resample(path, filename, config.demResolutions)
            print("Resampling done")
            filename_old = filename
            # Copy files to result folder
            # they are placed in ./processing/resampled/dem<resolution>/
            for resolution in config.demResolutions:
                # Update variable to match the changes done in resample
                filename = replace_last(filename_old, "dgm1", "dgm" + str(resolution))
                old_path = os.path.join(processing_path, "4_resampled", "dem" + str(resolution), filename)
                path = os.path.join(output_path, "dem" + str(resolution), filename) 

                if not os.path.exists( os.path.join(output_path, "dem" + str(resolution)) ):
                    print("Creating output subdir ./output/" + "dem" + str(resolution))
                    os.mkdir( os.path.join(output_path, "dem" + str(resolution)) )

                print("Copy resampled file to output directory")
                shutil.copy(old_path, path)

            print("File processed")
            print("--------------------------------------------")
        print("Script done.")
    except Exception as e:
        raise Exception(e)
    finally:
        print("Cleaning processing directory")
        shutil.rmtree(processing_path) # Could be done earlier if memory usage is an issue




def unzip(input_path, output_path):
    with gzip.open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
           shutil.copyfileobj(f_in, f_out)


def xyzToTiff(input_path, output_path):
    options = gdal.TranslateOptions(outputSRS=epsg25832) #no reproject, just stores the info in the tiff
    temp = gdal.Translate(output_path, input_path, options=options)
    temp = None # write to disk


def reprojectToWGS84(input_path, output_path):
    options = gdal.WarpOptions(srcSRS=epsg25832, dstSRS=epsg4326)
    temp = gdal.Warp(output_path, input_path, options=options)
    temp = None # write to disk


def noDataToMean(input_path):
    raster = gdal.Open(input_path, gdal.GA_Update)
    band = raster.GetRasterBand(1)
    # Calculate band mean
    array = raster.GetRasterBand(1).ReadAsArray().astype('float')
    # Exclude no data values
    nodata = band.GetNoDataValue()
    if nodata is not None:
        array = np.ma.masked_equal(array, nodata) # array is a masked array now
    mean = array.mean()
    array[array > 10000000] = mean # works even though the array is masked. nodata is a reeeealy large number for some reason
    band.WriteArray(array.data)


def resample(path, filename, resolutions):
    # Resolution is an array with the target values
    # Get pixes size, since it is in degree instead of meter now
    tiff = gdal.Open(path)
    gt = tiff.GetGeoTransform()
    pixel_size = gt[1]

    old_path = path
    filename_old = filename
    for resolution in resolutions:
        print(str(resolution) + "...")
        new_pixel_size = pixel_size * resolution
        
        # Files are placed in different folders anyway, but change the filename for consistency
        filename = replace_last(filename_old, "dgm1", "dgm" + str(resolution))
        path = os.path.join(processing_path, "4_resampled", "dem" + str(resolution), filename)

        if not os.path.exists(os.path.join(processing_path, "4_resampled", "dem" + str(resolution))):
                print("Creating subdir ./processing/4_resampled/dem" + str(resolution))
                os.mkdir(os.path.join(processing_path, "4_resampled", "dem" + str(resolution)))

        options = gdal.WarpOptions(
            xRes=new_pixel_size,
            yRes=new_pixel_size,
            resampleAlg='near'
        )
        temp = gdal.Warp(path, old_path, options=options)
        temp = None # write to disk


def replace_last(str, old, new):
    last_char_index = str.rfind(old)
    new_string = str[:last_char_index] + new + str[last_char_index+len(old):]
    return new_string

main()