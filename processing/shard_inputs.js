const {promises, createReadStream} = require('fs');

const {pointToTile, tileToGeoJSON} = require('@mapbox/tilebelt');
const { default: booleanContains } = require('@turf/boolean-contains');
const { default: area } = require('@turf/area');
const { default: intersect } = require('@turf/intersect');

const {join, extname} = require('path');
const Pick = require('stream-json/filters/Pick');
const {streamArray} = require('stream-json/streamers/StreamArray');

const sourceDir = './source', shardDir = './shards'
const ZOOM_LEVEL = 15;

/**
 * from https://graphics.stanford.edu/~seander/bithacks.html#InterleaveTableObvious
 * returns mortons number for provided tile
 */
function tileToZindex(x,y) {
    let zIndex = 0;
    const bounds = (Math.max(x,y) >>> 0).toString(2).length;

    for (let i = 0; i < bounds; ++i)
        zIndex |= (x & 1 << i) << i | (y & 1 << i) << (i + 1);

    return zIndex >>> 0;
}

/**
 * Adds the polygon(s) in the feature to the zIndex array they most intersect with
 * @param {object} chunk geojson polygon
 * @param {object} documentZIndexes hashmap of zIndex->array of features within zIndex 
 */
function processChunk(chunk, documentZIndexes) {
    // loop through each coordinate in the polygon
    // find and place it in the zIndex it fits into the most

    const trackIntersection = [0, 0] // [perc_intersection, zIndex]
    const tiles = {};
    for (const poly of chunk.geometry.coordinates) {
        for (const coord of poly) {
            const tile = pointToTile(coord[0], coord[1], ZOOM_LEVEL),
                  zIndex = tileToZindex(tile[0],tile[1]);

            if (tiles.hasOwnProperty(zIndex))
                continue;
            
            if (!documentZIndexes.hasOwnProperty(zIndex))
                documentZIndexes[zIndex] = []

            tiles[zIndex] = true;

            const bboxPoly = tileToGeoJSON(tile);

            // a chunk completely within has 100% intersection
            // otherwise, calculate area of the tile~chunk intersection / area of the tile
            const intersection = booleanContains(bboxPoly, chunk) 
                ? 100 : area(intersect(bboxPoly, chunk)) / area(bboxPoly); 
            
            if (trackIntersection[0] < intersection) {
                trackIntersection[0] = intersection;
                trackIntersection[1] = zIndex;
            }
        }
        documentZIndexes[trackIntersection[1]].push({
            feature: chunk.geometry,
            metadata: chunk.properties,
            zIndex: trackIntersection[1]
        });
    }
}
function processFile(filepath) {
    const documentZIndexes = {};

    return new Promise((resolve, reject) => {
        // create readstream in which each chunk is a geojson feature
        const featureStream = createReadStream(filepath)
            .pipe(Pick.withParser({filter:'features'}))
            .pipe(streamArray())

        featureStream.on('data', ({key,value}) => {
            console.log(`processing feature ${key}`)
            processChunk(value, documentZIndexes)
        });
        featureStream.on('error', () => reject());
        featureStream.on('end', async () => {
            let zIndexes = Object.keys(documentZIndexes).sort(),
                numServers = 1, 
                tilesPerServer = zIndexes.length / numServers,
                tileInc = 0, 
                featuresForServer = [];

            for (const zIndex of zIndexes) {
                if (tileInc === tilesPerServer) {
                    await promises.writeFile(`${shardDir}/server_${numServers}.json`, JSON.stringify(featuresForServer));
                    tileInc = 0;
                    --numServers;
                    featuresForServer = [];
                } else {
                    featuresForServer.push(...documentZIndexes[zIndex])
                    ++tileInc
                }
            }    
        
            if (numServers !== 0) {
                await promises.writeFile(`${shardDir}/server_${numServers}.json`, JSON.stringify(featuresForServer));
            }
        
            resolve()
        });

    })
}

async function main() {
    try {
        for (const file of await promises.readdir(sourceDir)) {
            if (extname(file) !== '.geojson')
                continue

            await processFile(join(sourceDir, file));       
        }
        console.log('processed files')
        process.exit(0);
    } catch (e) {
        process.exit(1);
    }
}

main()