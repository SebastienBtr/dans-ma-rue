const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run() {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    // Création de l'indice si non présent
    checkIndices(client, 'in-da-street');

    let dataSet = [];

    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            dataSet.push({
                "@timestamp": data.DATEDECL,
                "object_id": data.OBJECTID,
                "annee_declaration": data['ANNEE DECLARATION'],
                "mois_declaration": data['MOIS DECLARATION'],
                "type": data.TYPE,
                "sous_type": data.SOUSTYPE,
                "code_postal": data.CODE_POSTAL,
                "ville": data.VILLE,
                "arrondissement": data.ARRONDISSEMENT,
                "prefixe": data.PREFIXE,
                "intervenant": data.INTERVENANT,
                "conseil_de_quartier": data['CONSEIL DE QUARTIER'],
                "location": data.geo_point_2d
            });
        })
        .on('end', () => {
            console.log('push data');

            client.bulk(createBulkInsertQuery(dataSet), (err, resp) => {
                if (err) {
                    console.trace(err.message);
                } else {
                    console.log(`Inserted ${resp.body.items.length} dataSet`);
                }
                client.close();
            });

            console.log('Terminated!');
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(dataSet) {
    const body = dataSet.reduce((acc, data) => {
        acc.push({ index: { _index: 'imdb', _type: '_doc', _id: data.object_id } })
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
