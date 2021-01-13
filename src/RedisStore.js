const
	module_name = 'module.rdf.RedisStore',
	{ EventEmitter } = require('events'),
	// redis = require('redis'),
	{ promisify } = require('util'),
	dataFactory = require('../../module.persistence/src/module.persistence.js'),
	datasetFactory = require('../../module.persistence.inmemory/src/module.persistence.inmemory.js'),
	{ isQuad, isNamedNode, isLiteral, isSubject, isPredicate, isObject, termToString, termFromString } = dataFactory;

/**
 *
 * @param {redis~Client} client
 * @returns {Object<string, function>}
 * @private
 */
function _wrapClient(client) {
	const
		RE_test_method = /^[A-Z]+$/,
		/** @type {Array<[string, function]>} */
		clientProtoEntries = Object.entries(client.__proto__),
		/** @type {Object<string, function>} */
		clientAsyncWrapper = {};

	for (let [key, value] of clientProtoEntries) {
		if (RE_test_method.test(key) && typeof value === 'function') {
			clientAsyncWrapper[key] = promisify(value).bind(client);
		}
	}

	return clientAsyncWrapper;
} // _wrapClient

class RedisStore extends EventEmitter {

	// TODO make all methods transaction based

	#graph = null;
	#client = null;

	constructor(graph, client) {
		if (!isNamedNode(graph))
			throw new Error(`${module_name}#constructor : invalid graph`);
		if (!(client && typeof client === 'object' && typeof client.HGET === 'function'))
			throw new Error(`${module_name}#constructor : invalid client`);

		super();
		this.#graph = graph;
		this.#client = _wrapClient(client);
	} // RedisStore#constructor

	/**
	 * @returns {Promise<Number>}
	 */
	async size() {
		return this.#client.SCARD('?subject ?predicate ?object');
	} // RedisStore#size

	/**
	 * @param {Quad} quad
	 * @returns {Promise<Boolean>}
	 */
	async add(quad) {
		if (!isQuad(quad))
			throw new Error(`${module_name}#add : invalid quad`);

		const
			{ subject, predicate, object } = quad,
			subjKey = termToString(subject),
			predKey = termToString(predicate),
			objKey = termToString(object),
			quadKey = `${subjKey} ${predKey} ${objKey}`,
			addCount = await this.#client.HSET(quadKey,
				'termType', 'Quad',
				'subject', subjKey,
				'predicate', predKey,
				'object', objKey
			);

		if (addCount > 0) {
			await Promise.all([
				this.#client.SADD(`?subject ?predicate ?object`, quadKey),
				this.#client.SADD(`${subjKey} ?predicate ?object`, quadKey),
				this.#client.SADD(`?subject ${predKey} ?object`, quadKey),
				this.#client.SADD(`?subject ?predicate ${objKey}`, quadKey),
				this.#client.SADD(`${subjKey} ${predKey} ?object`, quadKey),
				this.#client.SADD(`${subjKey} ?predicate ${objKey}`, quadKey),
				this.#client.SADD(`?subject ${predKey} ${objKey}`, quadKey),
				this.#client.HSET(subjKey, 'termType', subject.termType, 'value', subject.value),
				this.#client.HSET(predKey, 'termType', predicate.termType, 'value', predicate.value),
				isLiteral(object)
					? this.#client.HSET(objKey, 'termType', object.termType, 'value', object.value, 'language', object.language, 'datatype', object.datatype.value)
					: this.#client.HSET(objKey, 'termType', object.termType, 'value', object.value)
			]);
			return true;
		} else {
			return false;
		}
	} // RedisStore#add

	/**
	 * @param {Quad} quad
	 * @returns {Promise<Boolean>}
	 */
	async delete(quad) {
		if (!isQuad(quad))
			throw new Error(`${module_name}#delete : invalid quad`);

		const
			{ subject, predicate, object } = quad,
			subjKey = termToString(subject),
			predKey = termToString(predicate),
			objKey = termToString(object),
			quadKey = `${subjKey} ${predKey} ${objKey}`,
			delCount = await this.#client.DEL(quadKey);

		if (delCount > 0) {
			await Promise.all([
				this.#client.SREM(`?subject ?predicate ?object`, quadKey),
				this.#client.SREM(`${subjKey} ?predicate ?object`, quadKey),
				this.#client.SREM(`?subject ${predKey} ?object`, quadKey),
				this.#client.SREM(`?subject ?predicate ${objKey}`, quadKey),
				this.#client.SREM(`${subjKey} ${predKey} ?object`, quadKey),
				this.#client.SREM(`${subjKey} ?predicate ${objKey}`, quadKey),
				this.#client.SREM(`?subject ${predKey} ${objKey}`, quadKey)
			]);
			return true;
		} else {
			return false;
		}
	} // RedisStore#delete

	/**
	 * @param {Quad} quad
	 * @returns {Promise<Boolean>}
	 */
	async has(quad) {
		if (!isQuad(quad))
			throw new Error(`${module_name}#has : invalid quad`);

		const
			{ subject, predicate, object } = quad,
			subjKey = termToString(subject),
			predKey = termToString(predicate),
			objKey = termToString(object),
			quadKey = `${subjKey} ${predKey} ${objKey}`,
			existCount = await this.#client.EXISTS(quadKey);

		return existCount > 0;
	} // RedisStore#has

	/**
	 * @param {Term} [subject]
	 * @param {Term} [predicate]
	 * @param {Term} [object]
	 * @param {Term} [graph]
	 * @returns {Promise<Dataset>}
	 */
	async match(subject, predicate, object) {
		if (subject && !isSubject(subject))
			throw new Error(`${module_name}#match : invalid subject`);
		if (predicate && !isPredicate(predicate))
			throw new Error(`${module_name}#match : invalid predicate`);
		if (object && !isObject(object))
			throw new Error(`${module_name}#match : invalid object`);

		const
			subjKey = subject ? termToString(subject) : '?subject',
			predKey = predicate ? termToString(predicate) : '?predicate',
			objKey = object ? termToString(object) : '?object',
			searchKey = `${subjKey} ${predKey} ${objKey}`,
			dataset = datasetFactory.dataset(),
			quadKeys = (subject && predicate && object)
				? [searchKey]
				: await this.#client.SMEMBERS(searchKey),
			termCache = new Map();

		await Promise.all(quadKeys.map(async (quadKey) => {
			try {
				const
					quadData = await this.#client.HGETALL(quadKey),
					[subject, predicate, object] = await Promise.all(
						[quadData.subject, quadData.predicate, quadData.object].map(async (key) => {
							if (termCache.has(key))
								return termCache.get(key);
							const node = await this.#client.HGETALL(key);
							termCache.set(key, node);
							return node;
						})
					),
					quad = dataFactory.fromQuad({
						termType: 'Quad',
						subject,
						predicate,
						object,
						graph: this.#graph
					});

				dataset.add(quad);

			} catch (err) {
				console.error(err);
			}
		}));

		return dataset;
	} // RedisStore#match


} // RedisStore

module.exports = RedisStore;