import pdal
import json
# The pipeline defines the steps for pdal
json_pipeline = {
    "pipeline": [{
             "type": "readers.e57",
             "filename": "./input/U_Bahn_Kreuzkirche_PW_georeferenziert.e57",
             "default_srs": "EPSG:4647"
        }, {
            "type":"filters.reprojection",
            "in_srs":"EPSG:4647",
            "out_srs":"EPSG:4326",
        }, {
             "type": "writers.las",
             "filename": "./output/U_Bahn_Kreuzkirche.las",
             "scale_x": "0.0000001",
             "scale_y": "0.0000001",
             "offset_x": "auto",
             "offset_y": "auto",
        }
    ]
}

pipeline = pdal.Pipeline( json.dumps(json_pipeline) )
pipeline.execute()

