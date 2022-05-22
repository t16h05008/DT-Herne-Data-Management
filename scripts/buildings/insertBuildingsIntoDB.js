/**
 * The 3D-buildings are exported from a 3DCityDB using the 3DCityDB Importer/Exporter.
 * This creates a folder structure containing the tiled 3D-models with their georeference.
 *
 * This Node.js script can be used to import such a file-structure into a MongoDB instance, converting it to the format needed by the database in the process.
 * The configuration details need to be specified in a separate file called 'insertBuildingsIntoDB.config.json'.
 * Optionally, building attributes can be imported if they were exported from the 3DCityDB as CSV.
 * The CSV file is expected to be present in the root directory of the folder structure.
 *
 * The 3D-Models are stored using GridFS. The georeference and tile information is stored in the metadata field of each building.
 * Additionally, a separate document is created in the database, which contains the tiling information.
 * This document can be queried by clients to determine, which models need to be queries in subsequent requests.
 * The file tileInfo is structured like this (example is never actually used in this script):
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

const MongoDB = require("mongodb");
const fs = require("fs");
const txml = require("txml");
const path = require("path");
const { Transform } = require('stream');
let csvToJson = require('convert-csv-to-json');
require("jsonminify");

let config;
const scriptFolderPath = path.dirname(__filename)
const configPath = path.resolve(scriptFolderPath, "config", "insertBuildingsIntoDB.config.json");

main();

function main() {
    console.log("Reading configuration");
    readConfigDetails();

    let mainKmlPath = path.join(
        "./input",
        config.cityDBExportMainKmlName
    );

    let fileContent = fs.readFileSync(mainKmlPath, "utf8");
    let parsed = txml.simplify(txml.parse(fileContent));

    let folders = parsed.kml.Document.Folder;
    // A folder can be seen as a tile here
    console.log("Creating tileInfo file");
    // Iterates folder structure
    let tileInfo = createTileInfoFile(folders);
    // Iterates folder structure again, abut as long as performance is not an issue it is ok.
    let buildings = createBuildings(folders);

    let attributesJson;
    if(config.insertBuildingsInfo) {
        console.log("Converting csv file to json");
        let inputDirContent = fs.readdirSync("./input");
        let csvFileName = inputDirContent.filter( function( elm ) {return elm.match(/.*\.(csv?)/ig);});
        csvFileName = csvFileName[0]; // Only allow one CSV file for now
        let csvFilePath = path.join("./input", csvFileName)
        attributesJson = convertCsvToJson(csvFilePath);
    }

    //const uri = "mongodb+srv://"+ username + ":" + password + "@" + clusterUrl  + "/test?retryWrites=true&w=majority";+
    mongoDbUrl = "mongodb://" + config.server + ":" + config.port; // TODO for development, has to be replaced later
    console.log(mongoDbUrl);
    const client = new MongoDB.MongoClient(mongoDbUrl);
    async function run() {
        try {
            // Connect the client to the server
            console.log("Connecting to database");
            await client.connect();
            // Establish and verify connection
            await client.db("admin").command({ ping: 1 });
            console.log("Writing buildings to database");
            await writeToDatabaseBucket(
                client.db(config.database),
                buildings
            );
            console.log("Writing tileInfo to database");
            await writeTileInfoToDatabase(
                client.db(config.database),
                tileInfo
            );

            if(config.insertBuildingsInfo) {
                console.log("Writing buildingsInfo to database");
                await writeBuildingsInfoToDatabase(
                    client.db(config.database),
                    attributesJson
                );
            }
            console.log("Script done");
        } finally {
            // Ensures that the client will close on finish/error
            await client.close();
        }
    }
    run().catch(console.dir);
}

// Read the configuration details
function readConfigDetails() {
    let exists = checkFileExistsSync(configPath)
    if(exists) {
        let fileContent = fs.readFileSync(
            configPath,
            "utf8"
        );
        config = JSON.parse(JSON.minify(fileContent));
        return;
    }
    throw new Error("Config file not found.")
}

// https://stackoverflow.com/a/35008327/18450475
function checkFileExistsSync(filepath){
    let flag = true;
    try{
      fs.accessSync(filepath, fs.constants.F_OK);
    }catch(e){
      flag = false;
    }
    return flag;
}

function createEntityForTileInfo(parsedFileContent) {
    let location = parsedFileContent.Model.Location;
    let orientation = parsedFileContent.Model.Orientation;
    return {
        id: parsedFileContent.name,
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
            "./input",
            folder.NetworkLink.Link.href
        );
        let tileKmlFileContent = fs.readFileSync(tileKmlPath, "utf8");
        let parsed = txml.simplify(txml.parse(tileKmlFileContent));
        let entities = parsed.kml.Document.Placemark;
        // Multiple entities --> Array
        // One entity --> Object
        if (entities.length >= 1) {
            for (let entity of entities) {
                tile.entities.push(createEntityForTileInfo(entity));
            }
        } else {
            let entity = entities;
            tile.entities.push(createEntityForTileInfo(entity));
        }
        tileInfo.tiles.push(tile);
    }
    return tileInfo;
}


function createBuildings(folders) {
    let buildings = [];
    for (let folder of folders) {
        let tileKmlPath = path.join(
            "./input",
            folder.NetworkLink.Link.href
        );
        let tileKmlFileContent = fs.readFileSync(tileKmlPath, "utf8");
        let parsed = txml.simplify(txml.parse(tileKmlFileContent));
        let entities = parsed.kml.Document.Placemark;
        // Multiple entities --> Array
        // One entity --> Object
        if (entities.length >= 1) {
            for (let entity of entities) {
                buildings.push(createEntityForBuilding(entity, tileKmlPath));
            }
        } else {
            let entity = entities;
            buildings.push(createEntityForBuilding(entity, tileKmlPath));
        }
    }
    return buildings;
}

function createEntityForBuilding(parsedFileContent, tileKmlUri) {
    let location = parsedFileContent.Model.Location;
    let orientation = parsedFileContent.Model.Orientation;
    let tileIndices = extractTileIndices(tileKmlUri);
    let absoluteModelPath = path.join(
        "./input",
        "Tiles",
        tileIndices[0].toString(),
        tileIndices[1].toString(),
        parsedFileContent.Model.Link.href
    );
    // TODO .gltb
    // Replace .dae with .gltf if needed
    if (absoluteModelPath.endsWith(".dae")) {
        absoluteModelPath = absoluteModelPath.replace(
            new RegExp(".dae$"),
            ".gltf"
        );
    }

    return {
        id: parsedFileContent.name,
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


/**
 * Writes the .gltf files to the database (in a bucket)
 * @param {*} db
 * @param {*} buildings
 * @returns
 */
async function writeToDatabaseBucket(db, buildings) {
    // create or get a bucket
    let bucket = new MongoDB.GridFSBucket(db, { bucketName: "buildings" });
    // For now we drop the bucket and recreate it
    bucket.drop();
    bucket = new MongoDB.GridFSBucket(db, { bucketName: "buildings" });

    let promises = [];
    for (let building of buildings) {
        let promise = new Promise((resolve, reject) => {
            // Read the file with fs and pipe the stream into the gridFS writer
            // fs.createReadStream is async, even though it looks like a synchronous function
            console.log("Writing building with id: ", building.id);

             const minifyJsonTransform = new Transform({
                transform(chunk, encoding, callback) {
                    this.push(chunk.toString('utf-8').replace(/[\n\r\s]+/g, ''));
                    callback();
                }
            });

            fs.createReadStream(building.pathToModel, 'utf-8')
                .pipe(minifyJsonTransform)
                .pipe(bucket.openUploadStream(building.pathToModel, {
                    chunkSizeBytes: 1048576,
                    // Store the georeference information in the metadata
                    metadata: {
                        id: building.id
                    },
                })
                .on("error", () => {
                    console.error("Something went wrong while writing building with id: ", building.id);

                    reject();
                })
                .on("finish", () => {
                    resolve();
                })
            );
        });

        promises.push(promise);
    }

    return await Promise.all(promises);
}


async function writeTileInfoToDatabase(db, data) {
    let collection = db.collection(config.collection + ".tileInfo");
    // Remove all data first
    await collection.deleteMany({});
    await collection.insertOne(data);
}

async function writeBuildingsInfoToDatabase(db, data) {
    let collection = db.collection(config.collection + ".attributes");
    // Remove all data first
    await collection.deleteMany({});
    for(let obj of data) {
        await collection.insertOne(obj);
    }
    
}


function extractTileIndices(path) {
    var regex = /(\\\d+\\\d+\\)(?!.*\1)/i;
    let substr = path.match(regex)[1];
    let split = substr.split("\\");
    return [parseInt(split[1]), parseInt(split[2])];
}


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

