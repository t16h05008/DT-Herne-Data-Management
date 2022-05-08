/**
 * The 3D-buildings are exported from a 3DCityDB using the 3DCityDB Importer/Exporter.
 * This creates a folder structure containing the tiled 3D-models with their georeference.
 *
 * This Node.js script can be used to import such a file-structure into a MongoDB instance, converting it to the format needed by the database in the process.
 * The configuration details need to be specified in a separate file called 'updateBuildingsDatabase.config.js'.
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
                // Location is the reference point where the local crs of the gltf-file
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

let config;

main();

function main() {
    readConfigDetails();

    let tileInfo = {
        tiles: []
    };
    let buildings = [];

    let mainKmlUri = path.join(
        config.cityDBExportRootFolderPath,
        config.cityDBExportMainKmlName
    );
    let fileContent = fs.readFileSync(mainKmlUri, "utf8");
    let parsed = txml.simplify(txml.parse(fileContent));

    let folders = parsed.kml.Document.Folder;
    // A folder can be seen as a tile here
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
        let tileKmlUri = path.join(
            config.cityDBExportRootFolderPath,
            folder.NetworkLink.Link.href
        );
        let tileKmlFileContent = fs.readFileSync(tileKmlUri, "utf8");
        let parsed = txml.simplify(txml.parse(tileKmlFileContent));
        let entities = parsed.kml.Document.Placemark;
        // Multiple entities --> Array
        // One entity --> Object
        if (entities.length >= 1) {
            for (let entity of entities) {
                tile.entities.push(createEntityFortileInfo(entity));
                buildings.push(createEntityForBuildings(entity, tileKmlUri));
            }
        } else {
            let entity = entities;
            tile.entities.push(createEntityFortileInfo(entity));
            buildings.push(createEntityForBuildings(entity, tileKmlUri));
        }

        tileInfo.tiles.push(tile);
    }

    //const uri = "mongodb+srv://"+ username + ":" + password + "@" + clusterUrl  + "/test?retryWrites=true&w=majority";+
    mongoDbUri = "mongodb://localhost:27017"; // for development, has to be replaced later
    const client = new MongoDB.MongoClient(mongoDbUri);
    async function run() {
        try {
            // Connect the client to the server
            await client.connect();
            // Establish and verify connection
            await client.db("admin").command({ ping: 1 });
            console.log("Connected to database");

            await writeToDatabaseBucket(
                client.db("DigitalerZwillingHerne"),
                buildings
            );
            await writeToDatabase(
                client.db("DigitalerZwillingHerne"),
                tileInfo
            );
            console.log("closing down");
        } finally {
            // Ensures that the client will close when you finish/error
            await client.close();
        }
    }
    run().catch(console.dir);
}

// Read the configuration details
function readConfigDetails() {
    let fileContent = fs.readFileSync(
        "./updateBuildingsDatabase.config.json",
        "utf8"
    );
    config = JSON.parse(fileContent);
}

function createEntityFortileInfo(parsedFileContent) {
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

function createEntityForBuildings(parsedFileContent, tileKmlUri) {
    let location = parsedFileContent.Model.Location;
    let orientation = parsedFileContent.Model.Orientation;
    let tileIndices = extractTileIndices(tileKmlUri);
    let absoluteModelPath = path.join(
        config.cityDBExportRootFolderPath,
        "Tiles",
        tileIndices[0].toString(),
        tileIndices[1].toString(),
        parsedFileContent.Model.Link.href
    );
    // replace .dae with .gltf if needed
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
            fs.createReadStream(building.pathToModel).pipe(
                bucket
                    .openUploadStream(building.pathToModel, {
                        chunkSizeBytes: 1048576,
                        // store the georeference information in the metadata
                        metadata: {
                            id: building.id,
                            location: building.location,
                            orientation: building.orientation,
                            tile: building.tile,
                        },
                    })
                    .on("error", () => {
                        console.error(
                            "Something went wrong while writing building with id: ",
                            building.id
                        );
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

/**
 * Writes the tileInfo json to the database
 * @param {*} db
 * @param {*} data
 */
async function writeToDatabase(db, data) {
    let collection = db.collection("buildings.tileInfo");
    // remove all data first
    await collection.deleteMany({});
    await collection.insertOne(data);
}

function extractTileIndices(path) {
    var regex = /(\\\d+\\\d+\\)(?!.*\1)/i;
    let substr = path.match(regex)[1];
    let split = substr.split("\\");
    return [parseInt(split[1]), parseInt(split[2])];
}
