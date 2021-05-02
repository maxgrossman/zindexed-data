const {pointToTile} = require('@mapbox/tilebelt');
const {tileToZIndex} = require('../lib/geos');

const {MongoClient} = require('mongodb');
const express = require('express');
const app = express();
const redis = require('redis');
const redisClient = redis.createClient();
const messages = {
    400: (reason) => `bad request: ${reason}`,
    500: () => 'server error'
}
const poiLookup = {
    'metadata.amenity': { $eq: 'restaurant' },
    'metadata.cuisine': { $eq: 'pizza'}
}


function redisMiddleware(req,res,next) {
    const key = `${req.path}?${Object.keys(req.query).sort().map(k => `${k}=${req.query[k]}`)}`
    redisClient.get(key, (error, cached) => {
        if (error) {
            res.status(500).json({
                message: messages[500]()
            })
        } else {
            if (cached === null) {
                next()
            } else {
                res.status(200).send(cached)
            }
        }
    })
}


app.get('/bbox', redisMiddleware, async (req, res) => {
    if (!req.query.hasOwnProperty('bounds')) {
        res.status(400).json({
            message: messages[400]('missing bounds query parameter')
        })
        return;
    }

    const findObject = {}, [w,s,e,n] = req.query.bounds.split(',').map(Number);
    
    if (req.query.approximate) {
        const northWestTile = pointToTile(w,n,15), southEastTile = pointToTile(e,s,15);
        findObject.zIndex = {
            $gte: tileToZIndex(northWestTile[0],northWestTile[1]),
            $lte: tileToZIndex(southEastTile[0],southEastTile[1])
        }
    } else {
        findObject.feature = {
            $geoIntersects: {
                $geometry: {
                    type: 'Polygon',
                    coordinates: [[
                        [w,n],  
                        [w,s],
                        [e,s],
                        [e,n],
                        [w,n]
                    ]]
                }
            }
        }
    }

    if (req.query.hasOwnProperty('poi') && poiLookup[req.query.poi]) {
        Object.keys(poiLookup[req.query.poi]).forEach(k => findObject[k] = poiLookup[req.query.poi][k]);
    }

    const features = await featuresCollection.find(findObject).toArray();
    const featureCollection = {
        type: 'FeatureCollection',
        features: features.map(f => {
            return {
                type: 'Feature',
                geometry: f.feature,
                propertyes: f.metadata
            }
        })
    }

    const key = `${req.path}?${Object.keys(req.query).sort().map(k => `${k}=${req.query[k]}`)}`
    redisClient.set(key, JSON.stringify(featureCollection));
    res.send(featureCollection);
})


async function main () {
    const client = new MongoClient('mongodb://0.0.0.0:27017');
    await client.connect();
    featuresCollection = (await client.db('zindex')).collection('features');

    app.listen('3000', () => console.log('service started at 3000'));
}

main()