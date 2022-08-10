// This script takes the output from "prepareBuildings.js", which is expected to be present in the ./output folder,
// and inserts it into the database. Configuration details are specified in ./config/insertBuildingsIntoDB.config.json

const Util = require("./_util.js");
const MongoDB = require("mongodb");
const fs = require("fs");
const path = require("path");
const { Transform } = require('stream');

let config;
const scriptFolderPath = path.dirname(__filename)
const configPath = path.resolve(scriptFolderPath, "config", "insertBuildingsIntoDB.config.json");

main();

function main() {

    console.log("Reading configuration");
    config = Util.readConfigDetails(configPath);

    console.log("Reading files");
    let fileContent = fs.readFileSync(path.join("./output", "tileInfo.json"), { encoding: "utf8"});
    let tileInfo = JSON.parse(fileContent);
    fileContent = fs.readFileSync(path.join("./output", "attributes.json"), { encoding: "utf8"});
    let attributesJson = JSON.parse(fileContent);

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
            console.log("Connection established");
            const db = client.db(config.database)
            console.log("Writing buildings to database");
            await writeToDatabaseBucket(db, tileInfo);
            console.log("Writing tileInfo.json to database");
            await writeTileInfoToDatabase(db, tileInfo);
            if(config.insertBuildingsInfo) {
                console.log("Writing buildingsInfo (attributes) to database");
                await writeBuildingsInfoToDatabase(db, attributesJson);
            }
            console.log("Script done");
        } finally {
            // Ensures that the client will close on finish/error
            await client.close();
        }
    }
    run().catch(console.dir);
}


/**
 * Writes the .gltf files to the database (in a bucket)
 * @param {*} db
 * @param {*} tileInfo
 * @returns
 */
async function writeToDatabaseBucket(db, tileInfo) {
    // create or get a bucket
    let bucket = new MongoDB.GridFSBucket(db, { bucketName: config.collection });
    // For now we drop the bucket and recreate it
    bucket.drop();
    bucket = new MongoDB.GridFSBucket(db, { bucketName: config.collection });

    let promises = [];
    for(let tile of tileInfo.tiles) {
        for (let building of tile.entities) {
            let promise = new Promise((resolve, reject) => {
                // Read the file with fs and pipe the stream into the gridFS writer
                // fs.createReadStream is async, even though it looks like a synchronous function
                console.log("Writing building with gmlId: ", building.gmlId);
    
                 const minifyJsonTransform = new Transform({
                    transform(chunk, encoding, callback) {
                        this.push(chunk.toString('utf-8').replace(/[\n\r\s]+/g, ''));
                        callback();
                    }
                });
                fs.createReadStream(building.pathToFile, 'utf-8')
                    .pipe(minifyJsonTransform)
                    .pipe(bucket.openUploadStream(building.pathToFile, {
                        chunkSizeBytes: 1048576,
                        metadata: {
                            id: building.id, // ascending numerical id
                            gmlId: building.gmlId
                            // No need to write geolocation here, the tileInfo.json file is already present in the client
                        },
                    })
                    .on("error", () => {
                        console.error("Something went wrong while writing building with gmlId: ", building.gmlId);
                        reject();
                    })
                    .on("finish", () => {
                        resolve();
                    })
                );
            });
            promises.push(promise);
        }
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
