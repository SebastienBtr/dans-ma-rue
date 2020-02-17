const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    client
        .search({
            index: indexName,
            body: {
                size: 0,
                aggs: {
                    arrondissement: {
                        terms: {
                            field: "arrondissement.keyword",
                            size: 20
                        }
                    }
                }
            }
        })
        .then(resp => {
            callback(resp.body.aggregations.arrondissement.buckets.map((data) => {
                return {
                    arrondissement: data.key,
                    count: data.doc_count
                };
            }));
        }).catch((err) => {
            console.log(err);
        });
}

exports.statsByType = (client, callback) => {
    client
        .search({
            index: indexName,
            body: {
                size: 0,
                aggs: {
                    type: {
                        terms: {
                            field: "type.keyword",
                            size: 5
                        },
                        aggs: {
                            sous_type: {
                                terms: {
                                    field: "sous_type.keyword",
                                    size: 5
                                }
                            }
                        }
                    }

                }
            }
        })
        .then(resp => {
            callback(resp.body.aggregations.type.buckets.map((data) => {
                return {
                    type: data.key,
                    count: data.doc_count,
                    sous_types: data.sous_type.buckets.map((sous_type) => {
                        return {
                            sous_type: sous_type.key,
                            count: sous_type.doc_count,
                        }
                    })
                };
            }));
        }).catch((err) => {
            console.log(err);
        });
}

exports.statsByMonth = (client, callback) => {
    client
        .search({
            index: indexName,
            body: {
                size: 0,
                aggs: {
                    mois_annee: {
                        terms: {
                            field: "mois_annee.keyword",
                            size: 10
                        }
                    }
                }
            }
        })
        .then(resp => {
            callback(resp.body.aggregations.mois_annee.buckets.map((data) => {
                return {
                    month: data.key,
                    count: data.doc_count
                };
            }));
        }).catch((err) => {
            console.log(err);
        });
}

exports.statsPropreteByArrondissement = (client, callback) => {
    client
        .search({
            index: indexName,
            body: {
                size: 0,
                query: {
                    bool: {
                        must: {
                            match: {
                                type: "Propreté"
                            }
                        }
                    }
                },
                aggs: {
                    arrondissement: {
                        terms: {
                            field: "arrondissement.keyword",
                            size: 3
                        }
                    }
                }
            }
        })
        .then(resp => {
            callback(resp.body.aggregations.arrondissement.buckets.map((data) => {
                return {
                    arrondissement: data.key,
                    count: data.doc_count
                };
            }));
        }).catch((err) => {
            console.log(err);
        });
}
