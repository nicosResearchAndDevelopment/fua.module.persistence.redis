const
	{ describe, it } = require('mocha'),
	expect = require('expect'),
	redis = require('redis'),
	dataFactory = require('../../module.persistence/src/module.persistence.js'),
	datasetFactory = require('../../module.persistence.inmemory/src/module.persistence.inmemory.js'),
	storeFactory = require('../src/module.persistence.redis.js'),
	config = {
		url: 'redis://localhost:6379/'
	};

describe('module.persistence.redis : DataStoreFactoryInterface', function() {

	// https://hub.docker.com/_/redis
	// docker run
	//     --publish=6379:6379
	//     --volume=C:/Users/spetrac/Fua/fua-js/lib/module.rdf/test/data/redis:/data
	//     --name=redis-store-test
	//     --detach
	//     redis redis-server

	let client, store, graph, quad_1, quad_2;

	before('construct the client, the store, a graph node and two quads', async function() {
		graph = dataFactory.namedNode(config.url);
		client = redis.createClient(graph.value);
		store = storeFactory.store(graph, client);
		quad_1 = dataFactory.quad(
			dataFactory.namedNode('http://example.com/subject'),
			dataFactory.namedNode('http://example.com/predicate'),
			dataFactory.namedNode('http://example.com/object'),
			graph
		);
		quad_2 = dataFactory.quad(
			quad_1.subject,
			quad_1.predicate,
			dataFactory.literal('Hello World', 'en'),
			graph
		);
	});

	it('should add the two quads to the store once', async function() {
		expect(await store.add(quad_1)).toBeTruthy();
		expect(await store.add(quad_2)).toBeTruthy();
		expect(await store.add(quad_1)).toBeFalsy();
		expect(await store.add(quad_2)).toBeFalsy();
	});

	it('should match the two added quads by their subject', async function() {
		/** @type {Dataset} */
		const dataset = await store.match(quad_1.subject);
		expect(dataset.has(quad_1)).toBeTruthy();
		expect(dataset.has(quad_2)).toBeTruthy();
	});

	it('should currently have a size of 2', async function() {
		expect(await store.size()).toBe(2);
	});

	it('should delete the first quad once', async function() {
		expect(await store.delete(quad_1)).toBeTruthy();
		expect(await store.delete(quad_1)).toBeFalsy();
	});

	it('should only have the second quad stored', async function() {
		expect(await store.has(quad_1)).toBeFalsy();
		expect(await store.has(quad_2)).toBeTruthy();
	});

	it('should match the remaining quad by its object', async function() {
		/** @type {Dataset} */
		const dataset = await store.match(null, null, quad_2.object);
		expect(dataset.has(quad_1)).toBeFalsy();
		expect(dataset.has(quad_2)).toBeTruthy();
	});

	it('should have a size of 0, after it deleted the second quad', async function() {
		await store.delete(quad_2);
		expect(await store.size()).toBe(0);
	});

});