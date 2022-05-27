/**
 * This script prepares the 3d buildings for the database import.
 * It combines buildings in multiple file formats and is somewhat tailored to the way, the data was provided.
 * 
 * The majority of 3buildings are exported from 3DCityDB using the 3DCityDB Importer/Exporter.
 * This creates a folder / tile  structure containing the 3D-models (.gltf) with their georeference (.kml).
 * Additionally, some buildings were provided as Sketch-up files. Because I couldn't figure out a way to export cityGML, these were exported as kml (kmz actually).
 * They now have to be integrated into the 3DCityDB tile structure, before importing them into the database.
 * The tile structure is used to determine, if a building should be shown or not, depending on the pov in the client.
 * 
 * The "input" folder is expected to have this folder structure:
 * - cityDbExport
 *      --> cityDB tile structure with .kml file on top level. Optional .csv file for attributes
 * - kml
 *      --> One folder for each building, folder name will be used as building name (sketch-up doesn't include it in the export properly).
 *          Folder should contain a .kml file and a subfolder, which contains the .dae (collada) file and another subfolder for textures.
 * 
 * The configuration details need to be specified in a separate file called 'prepareBuildings.config.json'.
 * Optionally, building attributes can be imported if they were exported from the 3DCityDB as CSV.
 * The CSV file is expected to be present in the root directory of the cityDbExport folder structure.
 *
 * The 3D-Models are stored using GridFS. The georeference and tile information is stored in the metadata field of each building.
 * Additionally, a separate document is created, which contains the tiling information (for quick access in the client).
 * This document can be queried by clients to determine, which models need to be queries in subsequent requests.
 * it structured like this (example variable is never used in this script):
 */
 const example = [
    // This object represents a tile
    {
        id: "bahnhofstrasse_Tile_0_6", // The tile id
        // Bounding box
        extent: {
            east: "7.225524623636661",
            north: "51.53807303162008",
            south: "51.537007721392065",
            west: "7.223917734104503",
        },
        // The entities (buildings) contained in this tile
        entities: [
            {
                id: "_Bahnhofstrasse53-1_BD.scwVaNciA9DrBK2a8BQg", // unique entity id (gmlid). Can be used by clients in further requests
                // Location is the reference point for the local crs of the gltf-file
                location: {
                    lon: 7.2217331,
                    lat: 51.5410184,
                    alt: 0.0,
                },
                orientation: {
                    heading: 358.6091396,
                },
            },
            {
                // Maybe more entities
            },
        ],
    },
    // More tiles here
];

const Util = require("./_util.js");
const fse = require('fs-extra');
const txml = require("txml");
const path = require("path");
const csvToJson = require('convert-csv-to-json');
const turfHelpers = require("@turf/helpers")
const { default: booleanPointInPolygon } = require("@turf/boolean-point-in-polygon")
const xmlJsonConverter = require('xml-js');
const { execSync } = require('child_process');


let config;
const scriptFolderPath = path.dirname(__filename)
console.log(__filename);
const configPath = path.resolve(scriptFolderPath, "config", "prepareBuildings.config.json");
let buildingIdCounter = 0; // Unique id that should be increased each time it is used (++buildingIdCounter)
// Stores the bboxes of all tiles in memory on first tile iteration to reduce io operations. 
let tilesBboxObj = {
    // 0: {
        // 0: [...]
        // 1: [...]
        // ...
    // },
    // 1: { ... }, 
    // ...
}

main();

function main() {
    console.log("Reading configuration");
    config = Util.readConfigDetails(configPath);
    const pathToColladaGltfConverter = path.resolve(config.colladaToGltfConverterPath);
    
    try {
        copyToOutputDir(path.join("./input", "cityDbExport"));
        tilesBboxObj = storeTileBboxesInMemory(tilesBboxObj);
        integrateKmlFilesIntoCityDbStructure(pathToColladaGltfConverter);
        let mainKmlPath = path.join("./output", config.cityDBExportMainKmlName);
        let fileContent = fse.readFileSync(mainKmlPath, "utf8");
        let parsed = txml.simplify(txml.parse(fileContent));
        // A folder can be seen as a tile here.
        // We are only interested in the folders that contain buildings.
        let folders = parsed.kml.Document.Folder; 

        console.log("Creating tileInfo file");
        let tileInfo = createTileInfoFile(folders);
        buildingIdCounter = 0; // Reset the counter because we iterated all buildings once and have to do it again.
        let buildings = createBuildings(folders);

        let attributesJson;
        if(config.cityDbExportAttributesExported) {
            attributesJson = prepareCityDbAttributes(buildings)
        }

        // All changes are made at this point (in memory).
        // Write stuff to output folder
        console.log("Writing output files");
        console.log("Writing buildings file (json)");
        let exportPath = path.join("./output", "buildings.json");
        try {
            fse.writeFileSync(exportPath, JSON.stringify(buildings));
        } catch (err) {
            console.error(err);
            throw err;
        }

        console.log("Writing tileInfo file (json)");
        exportPath = path.join("./output", "tileInfo.json");
        try {
            fse.writeFileSync(exportPath, JSON.stringify(tileInfo));
        } catch (err) {
            console.error(err);
            throw err;
        }

        if(config.cityDbExportAttributesExported) {
            console.log("Writing cityDB attributes file (json)");
            exportPath = path.join("./output", replaceLast(config.cityDBExportAttributesName, "csv", "") + "json")
            try {
                fse.writeFileSync(exportPath, JSON.stringify(attributesJson));
            } catch (err) {
                console.error(err);
                throw err;
            }
        }

        console.log("Script done.");
    } catch (e) {
        console.log("Something went wrong. Cleaning output directory.");
        // Clean output directory
        fse.readdir("./output", (err, files) => {
            if (err) throw err;
            for (const file of files) {
                fse.removeSync(file)
            }
        });
        throw(e)
    }
}

// Read the configuration details
function readConfigDetails() {
    let exists = checkFileExistsSync(configPath)
    if(exists) {
        let fileContent = fse.readFileSync(configPath,"utf8");
        return JSON.parse(JSON.minify(fileContent));
    }
    throw new Error("Config file not found.")
}



function createTileInfoFile(folders) {
    let tileInfo = {
        tiles: []
    };

    for (let folder of folders) {
        let tile = {
            id: folder.name,
            extent: {
                east: parseFloat(folder.NetworkLink.Region.LatLonAltBox.east),
                west: parseFloat(folder.NetworkLink.Region.LatLonAltBox.west),
                north: parseFloat(folder.NetworkLink.Region.LatLonAltBox.north),
                south: parseFloat(folder.NetworkLink.Region.LatLonAltBox.south),
            },
            entities: [],
        };
        // For each tile, iterate the entities and add them, too.
        let tileKmlPath = path.join(
            "./output",
            folder.NetworkLink.Link.href
        );
        let tileKmlFileContent = fse.readFileSync(tileKmlPath, "utf8");
        let parsed = txml.simplify(txml.parse(tileKmlFileContent));
        let placemarks = parsed.kml.Document.Placemark;
        if(Array.isArray(placemarks)) {
            // Placemarks can be entities or tile borders, we need the entities.
            let entities = placemarks.filter( placemark => {
                return placemark.hasOwnProperty("Model");
            });
            for (let entity of entities)
                tile.entities.push(createEntityForTileInfo(entity));
        } else{
            // If there is only one placemark the prop is an object
            if(placemarks.hasOwnProperty("Model")) {
                tile.entities.push(createEntityForTileInfo(placemarks));
            }
        }
        tileInfo.tiles.push(tile);
    }
    return tileInfo;
}


function createBuildings(folders) {
    let buildings = [];
    for (let folder of folders) {
        let tileKmlPath = path.join(
            "./output",
            folder.NetworkLink.Link.href
        );
        let tileKmlFileContent = fse.readFileSync(tileKmlPath, "utf8");
        let parsed = txml.simplify(txml.parse(tileKmlFileContent));
        let placemarks = parsed.kml.Document.Placemark;
        if(Array.isArray(placemarks)) {
            // Placemarks can be entities or tile borders, we need the entities.
            let entities = placemarks.filter( placemark => {
                return placemark.hasOwnProperty("Model");
            });
            for (let entity of entities)
            buildings.push(createEntityForBuilding(entity, tileKmlPath));
        } else{
            // If there is only one placemark the prop is an object
            if(placemarks.hasOwnProperty("Model")) {
                buildings.push(createEntityForBuilding(placemarks, tileKmlPath));
            }
        }
    }
    return buildings;
}

/**
 * Creates an entity object to insert in the tileInfo file.
 * @param {object} parsedFileContent | The entity in the tile kml file
 * @returns entity object
 */
function createEntityForTileInfo(parsedFileContent) {
    let location = parsedFileContent.Model.Location;
    let orientation = parsedFileContent.Model.Orientation;
    return {
        id: ++buildingIdCounter,
        gmlId: parsedFileContent.name,
        location: {
            lon: parseFloat(location.longitude),
            lat: parseFloat(location.latitude),
            height: parseFloat(location.altitude),
        },
        orientation: {
            /* 
                The file only contains the heading
                No idea why +90 (degree) is needed here, but it works.
                Maybe the heading get exported with a different reference to the one used by Cesium from the db?
            */
            heading: parseFloat(orientation.heading) + 90,
        },
    };
}


function createEntityForBuilding(parsedFileContent, tileKmlUri) {
    let location = parsedFileContent.Model.Location;
    let orientation = parsedFileContent.Model.Orientation;
    let tileIndices = extractTileIndices(tileKmlUri);
    let absoluteModelPath = path.join(
        "./output",
        "Tiles",
        tileIndices[0].toString(),
        tileIndices[1].toString(),
        parsedFileContent.Model.Link.href
    );
    // TODO .gltb
    // Replace .dae with .gltf if needed
    if(absoluteModelPath.endsWith(".dae")) {
        absoluteModelPath = replaceLast(absoluteModelPath, ".dae", ".gltf");
    }

    return {
        id: ++buildingIdCounter,
        gmlId: parsedFileContent.name,
        location: {
            lon: parseFloat(location.longitude),
            lat: parseFloat(location.latitude),
            height: parseFloat(location.altitude),
        },
        orientation: {
            /* 
                The file only contains the heading
                No idea why +90 (degree) is needed here, but it works.
                Maybe the heading get exported with a different reference to the one used by Cesium from the db?
            */
            heading: parseFloat(orientation.heading) + 90,
        },
        pathToModel: absoluteModelPath,
        tile: tileIndices,
    };
}

// Get tile indices from cityDB folder path
function extractTileIndices(path) {
    console.log(path);
    var regex = /(\\\d+\\\d+\\)(?!.*\1)/i;
    let substr = path.match(regex)[1];
    let split = substr.split("\\");
    return [parseInt(split[1]), parseInt(split[2])];
}

// Convert the csv attributes into json
function convertCsvToJson(csvFilePath) {
    let json = csvToJson
        .utf8Encoding()
        .fieldDelimiter(config.csvFieldDelimiter)
        .getJsonFromCsv(csvFilePath);
    // The csv was exported wit surrounding quotes around every prop.
    // The conversion to json added more quotes, so we have to remove them.
    let regexStart = /^\"/;
    let regexEnd = /\"$/
    for(let obj of json) {
        for(let key in obj) {
            let newKey = key.replace(regexStart, "");
            newKey = newKey.replace(regexEnd, "");
            delete Object.assign(obj, {[newKey]: obj[key] })[key];

            if(typeof  obj[newKey] === "string") {
                obj[newKey] = obj[newKey].replace(regexStart, "");
                obj[newKey] = obj[newKey].replace(regexEnd, "");
            }
        }
    }
    return json
}

/**
 * Integrates each building, that was exported as kml from sketch-upm into the cityDB folder / tile structure.
 */
function integrateKmlFilesIntoCityDbStructure(pathToColladaGltfConverter) {
    // Needed later
    let cityDbTileContent = fse.readFileSync(path.join("./output", replaceLast(config.cityDBExportMainKmlName, "kml", "json")));
    let cityDbTileJson = JSON.parse(cityDbTileContent);

    let folderPath = path.join("./input", "kml");
    files = fse.readdirSync(folderPath, { withFileTypes: true });
    for(let file of files) {
        if( !file.isDirectory())
            continue; // Skip everything that is not a folder
        // Each folder represents one 3D-Model
        // We assume that only one model / building was exported in this folder
        let folderName = file.name;
        // Get the folder contents. It should contain one directory and one kml file
        let dirContent = fse.readdirSync(path.join(folderPath, folderName));
        let kmlFileName = dirContent.filter( function( elem ) {return elem.match(/.*\.(kml?)/ig);})[0];
        let kmlFileContent = fse.readFileSync(path.join(folderPath, folderName, kmlFileName), "utf8");
        let parsed = txml.simplify(txml.parse(kmlFileContent));
        // Get the georeference
        let model = parsed.kml.Folder.Placemark.Model;
        let georeference = {
            lat: model.Location.latitude,
            lon: model.Location.longitude,
            altitude: model.Location.altitude, // Should be 0
            heading: model.Orientation.heading // Tilt and roll shouldn't matter
        }

        // Might drop one or two decimal places, but 14-15 decimal places is still way more than we need with lon lat
        let pointWkt = turfHelpers.point([parseFloat(georeference.lon), parseFloat(georeference.lat)]);
        // Check which tile the model belongs to.
        // Only checking single coordinate, not bbox.
        // There could be some inaccuracies here if the model overlaps with other tiles.
        let numberTilesX = Object.keys(tilesBboxObj).length;
        for(let x=0; x<numberTilesX; x++) {
            let numberTilesY = Object.keys(tilesBboxObj[x]).length;
            for(let y=0; y<numberTilesY; y++) {
                let bbox = tilesBboxObj[x][y];
                let poly = bbox.geometry.coordinates[0];
                let north = poly[2][1];
                let south = poly[0][1];
                let east = poly[1][0];
                let west = poly[0][0];
                let bboxContainsModel = booleanPointInPolygon(pointWkt, bbox); // turf method
                if(bboxContainsModel) {
                    console.log("Model " + folderName + " found in tile " + x, y);
                    console.log("Updating folder structure to include it in tile structure.");
                    console.log("Updating tile kml");
                    // Tile kml file
                    let fileName = replaceLast(config.cityDBExportMainKmlName, ".kml", "") + "_Tile_" + x + "_" + y + "_collada.kml";
                    let p = path.join("./output", "Tiles", x.toString(), y.toString(), fileName);
                    let fileContent = fse.readFileSync(p, 'utf8');
                    let tileParsed = txml.simplify(txml.parse(fileContent, {
                        keepComments: true
                    }));
                    let tilePlacemarks = tileParsed.kml.Document.Placemark;
                    // If it is in object (no other buildings in this tile so far) we create an array
                    if(!Array.isArray(tilePlacemarks)) {
                        let arr = [];
                        arr.push(tilePlacemarks);
                        tileParsed.kml.Document.Placemark = arr;
                    }
                    let newPlacemark = {
                        _attributes: {
                            id: folderName
                        },
                        name: folderName,
                        Model: {
                            altitudeMode: "relativeToGround",
                            Location: {
                                longitude: georeference.lon,
                                latitude: georeference.lat,
                                altitude: georeference.altitude,
                            },
                            Orientation: {
                                heading: georeference.heading,
                            },
                            Link: {
                                href: folderName + "/" + folderName + ".gltf" // maybe forward slash is important here
                            }
                        }
                    }
                    tileParsed.kml.Document.Placemark.push(newPlacemark);
                    
                    delete tileParsed["?xml"]
                    tileParsed["_declaration"] = {
                        _attributes: {
                            version: "1.0",
                            encoding: "UTF-8",
                            standalone: "yes"
                        }
                    }
                    // order keys alphabetically to have the header on top again
                    tileParsed = Object.keys(tileParsed).sort().reduce(
                        (obj, key) => { 
                          obj[key] = tileParsed[key]; 
                          return obj;
                        }, 
                        {}
                    );
                    let xml = xmlJsonConverter.json2xml(tileParsed, {
                        spaces: 2,
                        compact: true
                    });
                    fse.writeFileSync(p, xml, {encoding:'utf8'});

                    console.log("Converting model to glTF and copy it into output folder");
                    let modelPath = path.join("./input", "kml", folderName, "models");
                    dirContent = fse.readdirSync(modelPath, {withFileTypes: true});
                    let colladaFile = dirContent.filter( function( elem ) {return elem.name.match(/.*\.(dae?)/ig);})[0];
                    let inputPath = path.join(modelPath, colladaFile.name)
                    let outputPath = path.join("./output", "Tiles", x.toString(), y.toString(), folderName, folderName + ".gltf");
                    let cmd = "\"" + pathToColladaGltfConverter + "\" --input \"" + inputPath + "\" --output \"" + outputPath + "\""
                    console.log("command: " + cmd);
                    execSync(cmd, (err, stdout, stderr) => {
                        if (err) {
                          console.log("Error while trying to run subcommand");
                          console.error(stderr);
                          return;
                        }
                    });

                    console.log("Updating root folder files");
                    p = path.join("./output", config.cityDBExportMainKmlName)
                    let rootKmlContent = fse.readFileSync(p, {encoding: "utf8"});
                    let parsed = txml.simplify(txml.parse(rootKmlContent));
                    let folders = parsed.kml.Document.Folder;
                    let folderExists = false;
                    for(let folder of folders) {
                        let regex = /(\d+\d+)/gi
                        let match = folder.name.match(regex);
                        if(parseInt(match[0]) === x && parseInt(match[1]) === y) {
                            folderExists = true;
                            break;
                        }
                    }

                    if(!folderExists) {
                        let name = replaceLast(config.cityDBExportMainKmlName, ".kml", "") + "_Tile_" + x + "_" + y;
                        let hrefPath = "Tiles/" + x + "/" + y + "/" + name + "_collada.kml"
                        let newFolder = {
                            name: name,
                            NetworkLink: {
                                name: "Display as collada",
                                Region: {
                                    LatLonAltBox: {
                                        north: north,
                                        south: south,
                                        east: east,
                                        west: west,
                                    },
                                    Lod: {
                                        minLodPixels: -1.0, // Not relevant for now
                                        maxLodPixels: -1.0,
                                    }
                                },
                                Link: {
                                    href: hrefPath,
                                    viewRefreshMode: "onRegion",
                                    viewFormat: ""
                                }
                            }
                        }
                        folders.push(newFolder);
                        delete parsed["?xml"]
                        parsed["_declaration"] = {
                            _attributes: {
                                version: "1.0",
                                encoding: "UTF-8",
                            }
                        }
                    }
                    
                    // order keys alphabetically to have the header on top again
                    parsed = Object.keys(parsed).sort().reduce(
                        (obj, key) => { 
                          obj[key] = parsed[key]; 
                          return obj;
                        }, 
                        {}
                    );
                    xml = xmlJsonConverter.json2xml(parsed, {
                        spaces: 2,
                        compact: true
                    });
                    fse.writeFileSync(p, xml, {encoding:'utf8'});

                    // And the root json file
                    p = path.join("./output", replaceLast(config.cityDBExportMainKmlName, ".kml", ".json"));
                    let rootJsonContent = fse.readFileSync(p, {encoding: "utf8"});
                    parsed = JSON.parse(rootJsonContent);
                    let exists = false;
                    for(let [key, value] of Object.entries(parsed)) {
                        if(value.tile[0] === x && value.tile[1] === y) {
                            exists = true;
                        }
                    }

                    if(!exists) {
                        parsed[folderName] = {
                            envelope: [west, south, east, north],
                            tile: [x, y]
                        }
                        fse.writeFileSync(p, JSON.stringify(parsed), {encoding: "utf8"})
                    }
                }
            }
        }
    }
}


function prepareCityDbAttributes(buildings) {
    console.log("Preparing cityDB attributes");
    console.log("Converting to JSON");
    let csvFilePath = path.join("./output", config.cityDBExportAttributesName)
    let attributesJson = convertCsvToJson(csvFilePath);
    // For each building, check if we exported any attributes
    console.log("Adding id");
    for(let building of buildings) {
        for(let attrSet of attributesJson) {
            if(building.gmlId === attrSet.GMLID) {
                // If yes, add the numerical building id as an attribute (so we can query attributes by building is later)
                attrSet["id"] = building.id;
                break;
            }
        }
    }
    return attributesJson;
}



function replaceLast(str, old, new_) {
    let n = str.lastIndexOf(old);
    return str.substring(0, n) + new_ + str.substring(n+old.length-1, str.length-1);
}


/**
 * Copies the specified folder to the root output dir
 * @param {*} path 
 */
function copyToOutputDir(path) {
    fse.copySync(path, "./output", { overwrite: true }, function (err) {
        if (err)
          console.error(err);
    });
}


function storeTileBboxesInMemory(tilesBboxObj) {
    let x=0;
    let y=0;
    let xFoldersCount = fse.readdirSync(path.join("./output", "Tiles")).length;
    let yFoldersCount;
    while(x >= 0) {
        if(x >= xFoldersCount)
            break; // Last x folder iterated

        tilesBboxObj[x] = {};
        yFoldersCount = fse.readdirSync(path.join("./output", "Tiles", x.toString())).length;
        while(y >= 0) {
            if(y >= yFoldersCount) {
                y = 0;
                break; // Go to next x folder
            }

            let fileName = replaceLast(config.cityDBExportMainKmlName, ".kml", "") + "_Tile_" + x + "_" + y + "_collada.kml";
            let p = path.join("./output", "Tiles", x.toString(), y.toString(), fileName);
            let fileContent = fse.readFileSync(p, 'utf8');
            let parsedContent = txml.simplify(txml.parse(fileContent, {
                filter: (node) => {
                    return node.tagName === "coordinates"; // should only exist for tile border placemark
                }
            }));
            let bbox = parsedContent.coordinates.split(" "); // south/west, south/east, north/east, north/west
            bbox = turfHelpers.polygon([[
                bbox[0].split(",").map(str => parseFloat(str)),
                bbox[1].split(",").map(str => parseFloat(str)),
                bbox[2].split(",").map(str => parseFloat(str)),
                bbox[3].split(",").map(str => parseFloat(str)),
                bbox[0].split(",").map(str => parseFloat(str)),
            ]]);
            tilesBboxObj[x][y] = bbox
            y++;
        }
        x++;
    }

    return tilesBboxObj
}