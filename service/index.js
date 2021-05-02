const {pointToTile} = require('@mapbox/tilebelt');
const {tileToZIndex} = require('../lib/geos');

const {MongoClient} = require('mongodb');
const express = require('express');
const app = express(); app.use(express.json());
const redis = require('redis');
const redisClient = redis.createClient();
const messages = {
    400: (reason) => `bad request: ${reason}`,
    500: () => 'server error'
}
const poiLookup = {
    pizza: {
        'metadata.amenity': { $eq: 'restaurant' },
        'metadata.cuisine': { $eq: 'pizza' }, 
    }
}

function redisMiddleware(req,res,next) {
redisClient.get(JSON.stringify(req.body), (error, cached) => {
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


app.post('/', redisMiddleware, async (req, res) => {
    if (!req.body.hasOwnProperty('geoFilter')) {
        res.status(400).json({
            message: message[400]('missing filter entry in request body')
        })
    } else {
        try {
            const findObject = {}
            switch (req.body.geoFilter) {
                case 'zIndex': {
                    const nwTile = pointToTile(req.body.bounds[0],req.body.bounds[3],15),
                          seTile = pointToTile(req.body.bounds[2],req.body.bounds[1],15);
                    
                    findObject.zIndex = {
                        $gte: tileToZIndex(nwTile[0],nwTile[1]),
                        $lte: tileToZIndex(seTile[0],seTile[1])
                    }
                    break;
                }
                case 'geojson': {
                    findObject.feature = {
                        $geoIntersects: {
                            $geometry: req.body.geometry
                        }
                    }
                    break
                }
                default: {
                    res.status(400).json({
                        message: message[400]('unkown geoFilter type')
                    })
                    return;
                }
            }

            if (req.body.hasOwnProperty('poiType') && poiLookup[req.body.poiType])
                Object.keys(poiLookup[req.body.poiType]).forEach(k => findObject[k] = poiLookup[req.body.poiType][k]);


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

            if (featureCollection.features.length)
                redisClient.set(JSON.stringify(req.body), JSON.stringify(featureCollection));
            
            res.send(featureCollection);
        } catch (e) {
            res.status(500).json({message: messages[500]() })
        }
    }
})


async function main () {
    const client = new MongoClient('mongodb://0.0.0.0:27017');
    await client.connect();
    featuresCollection = (await client.db('zindex')).collection('features');

    app.listen('3000', () => console.log('service started at 3000'));
}

main()