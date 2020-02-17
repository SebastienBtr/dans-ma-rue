const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    client
        .count({
            index: indexName,
            body: {
                query: {
                    range: {
                        "@timestamp": {
                            gte: from,
                            lt: to
                        }
                    }
                }
            }
        })
        .then(resp => {
            callback({
                count: resp.body.count
            });
        }).catch((err) => {
            console.log(err);
        });
}

exports.countAround = (client, lat, lon, radius, callback) => {
    client
        .count({
            index: indexName,
            body: {
                query: {
                    bool: {
                        must: {
                            match_all: {}
                        },
                        filter: {
                            geo_distance: {
                                distance: radius,
                                location: `${lat},${lon}`
                            }
                        }
                    }
                }
            }
        })
        .then(resp => {
            callback({
                count: resp.body.count
            });
        }).catch((err) => {
            console.log(err);
        });
}