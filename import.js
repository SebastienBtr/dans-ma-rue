const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');
const { chunk } = require('lodash');

async function run() {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    // Create index
    checkIndices(client, indexName);

    let dataSet = [];

    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            dataSet.push({
                '@timestamp': data.DATEDECL,
                'object_id': data.OBJECTID,
                'annee_declaration': data['ANNEE DECLARATION'],
                'mois_declaration': data['MOIS DECLARATION'],
                'type': data.TYPE,
                'sous_type': data.SOUSTYPE,
                'code_postal': data.CODE_POSTAL,
                'ville': data.VILLE,
                'arrondissement': data.ARRONDISSEMENT,
                'prefixe': data.PREFIXE,
                'intervenant': data.INTERVENANT,
                'conseil_de_quartier': data['CONSEIL DE QUARTIER'],
                'location': data.geo_point_2d
            });
        })
        .on('end', () => {
            console.log('push data');
            sendData(dataSet, client).then(() => {
                console.log('End');
                client.close();
            }, (err) => {
                console.log('End because of an error');
                console.trace(err);
                client.close();
            });
        });
}

async function sendData(dataSet, client) {
    return new Promise(async function (resolve, reject) {
        const chunks = chunk(dataSet, 20000);
        for (data of chunks) {
            const { body: bulkResponse } = await client.bulk(createBulkInsertQuery(data));
            if (bulkResponse.errors) {
                reject(errors);
            } else {
                console.log(`${data.length} data send`);
            }
        }
        resolve();
    });
}

// Format data for a bulk insert
function createBulkInsertQuery(dataSet) {
    const body = dataSet.reduce((acc, data) => {
        acc.push({ index: { _index: indexName, _type: '_doc', _id: data.object_id } })
        acc.push(data);
        return acc
    }, []);

    return { body };
}

// Create an index if not exist
function checkIndices(client, name) {
    client.indices.exists({ index: name }, (err, res, status) => {
        if (res) {
            console.log('index already exists');
        } else {
            client.indices.create({ index: name }, (err, res, status) => {
                console.log(err, res, status);
            });
        }
    });
}

run().catch(console.error);
