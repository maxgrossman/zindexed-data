const {MongoClient} = require('mongodb');
const client = new MongoClient('mongodb://0.0.0.0:27017');
const shardDir = './shards';
const shardNum = process.env[3] || 1
const {promises} = require('fs');
const { join, extname } = require('path');


// this is niave but concept is shardNum maps to the node that would be represent data in `server_${shardNum}.json` file
async function main() {
    try {
        await client.connect();
        const shardCollection = (await client.db('zindex')).collection('features');
        const features = JSON.parse((await promises.readFile(join(shardDir,`server_${shardNum}.json`))).toString());
        await shardCollection.insertMany(features);
        shardCollection.createIndex({feature: '2dsphere'})
        process.exit(0)
    } catch (e) {
        process.exit(1);
    }
}

main()