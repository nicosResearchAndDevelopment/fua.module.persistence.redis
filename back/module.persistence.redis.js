throw new Error('module.persistence.redis : not compatible with current module.persistence');

const
    // dataFactory = require('../../module.persistence/src/module.persistence.js'),
    // datasetFactory = require('../../module.persistence.inmemory/src/module.persistence.inmemory.js'),
    RedisStore = require('./RedisStore.js');

/**
 * @param {NamedNode} graph
 * @para {RedisClient} client
 * @returns {RedisStore}
 */
exports.store = function (graph, client) {
    return new RedisStore(graph, client);
};