const
	// dataFactory = require('../../module.persistence/src/module.persistence.js'),
	// datasetFactory = require('../../module.persistence.inmemory/src/module.persistence.inmemory.js'),
	RedisStore = require('./RedisStore.js');

/**
 * @param {NamedNode} graph
 * @para {RedisClient} db
 * @returns {RedisStore}
 */
exports.store = function(graph, client) {
	return new RedisStore(graph, client);
};