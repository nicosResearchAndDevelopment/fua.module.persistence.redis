const
    util                 = require('@nrd/fua.core.util'),
    assert               = new util.Assert('module.persistence.redis'),
    redis                = require('redis'),
    {DataStore}          = require('@nrd/fua.module.persistence'),
    {promisify}          = require('util'),
    isRedisCommand       = new util.StringValidator(/^[A-Z ]+$/),
    promisifyRedisClient = (redisClient) => Object.fromEntries(Object.entries(redisClient.__proto__)
        .filter(([command, method]) => isRedisCommand(command) && util.isFunction(method))
        .map(([command, method]) => [command, promisify(method).bind(redisClient)])
    ),
    crypto               = require('crypto'),
    termIdToKey          = (value) => crypto.createHash('sha1').update(value).digest('base64');

class RedisStore extends DataStore {

    /** @type {{[key: string]: function(...any): Promise<any>}} */
    #client = null;

    constructor(options, factory) {
        super(options, factory);
        const {url, user, password, db} = options;
        assert(util.isString(url), 'RedisStore#constructor : invalid url');
        assert(!db || util.isString(db), 'MongoDBStore#constructor : invalid user');
        assert(!user || util.isString(user), 'MongoDBStore#constructor : invalid user');
        assert(!password || util.isString(password), 'MongoDBStore#constructor : invalid password');
        const redisClient = redis.createClient({url, user, password, db});
        this.#client      = promisifyRedisClient(redisClient);
    } // RedisStore#constructor

    async size() {
        try {
            return await this.#client.SCARD('?subject ?predicate ?object');
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // RedisStore#size

    async match(subject, predicate, object, graph) {
        assert(!graph || this.factory.isDefaultGraph(graph), 'RedisStore#match : the graph option is not supported');

        const
            dataset   = await super.match(subject, predicate, object, graph),
            subjKey   = subject ? termIdToKey(this.factory.termToId(subject)) : '?subject',
            predKey   = predicate ? termIdToKey(this.factory.termToId(predicate)) : '?predicate',
            objKey    = object ? termIdToKey(this.factory.termToId(object)) : '?object',
            searchKey = `${subjKey} ${predKey} ${objKey}`;

        try {
            const
                quadKeys  = (subject && predicate && object)
                    ? [searchKey]
                    : await this.#client.SMEMBERS(searchKey),
                termCache = new Map();

            await Promise.all(quadKeys.map(async (quadKey) => {
                try {
                    const
                        quadData                      = await this.#client.HGETALL(quadKey),
                        [subjNode, predNode, objNode] = await Promise.all(
                            [quadData.subject, quadData.predicate, quadData.object].map(async (key) => {
                                if (termCache.has(key))
                                    return termCache.get(key);
                                const node = await this.#client.HGETALL(key);
                                termCache.set(key, node);
                                return node;
                            })
                        ),
                        quad                          = this.factory.fromQuad({
                            subject:   subjNode,
                            predicate: predNode,
                            object:    objNode
                        });
                    dataset.add(quad);
                } catch (err) {
                    this.emit('error', err);
                }
            }));

            return dataset;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // RedisStore#match

    async add(quads) {
        const
            quadArr = await super.add(quads);

        try {
            let quadsAdded = 0;

            await Promise.all(quadArr.map(async (quad) => {
                const
                    {subject, predicate, object} = quad,
                    subjKey                      = termIdToKey(this.factory.termToId(subject)),
                    predKey                      = termIdToKey(this.factory.termToId(predicate)),
                    objKey                       = termIdToKey(this.factory.termToId(object)),
                    quadKey                      = `${subjKey} ${predKey} ${objKey}`,
                    addCount                     = await this.#client.HSET(quadKey,
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
                        this.factory.isLiteral(object)
                            ? this.#client.HSET(objKey, 'termType', object.termType, 'value', object.value, 'language', object.language, 'datatype', object.datatype.value)
                            : this.#client.HSET(objKey, 'termType', object.termType, 'value', object.value)
                    ]);

                    quadsAdded++;
                    this.emit('added', quad);
                }
            }));

            return quadsAdded;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // RedisStore#add

    async addStream(stream) {
        const
            quadStream = await super.addStream(stream),
            quadArr    = [];

        await new Promise((resolve) => {
            quadStream.on('data', quad => quadArr.push(quad));
            quadStream.on('end', resolve);
        });

        return this.add(quadArr);
    } // RedisStore#addStream

    async delete(quads) {
        const quadArr = await super.delete(quads);

        try {
            let quadsDeleted = 0;

            await Promise.all(quadArr.map(async (quad) => {
                const
                    {subject, predicate, object} = quad,
                    subjKey                      = termIdToKey(this.factory.termToId(subject)),
                    predKey                      = termIdToKey(this.factory.termToId(predicate)),
                    objKey                       = termIdToKey(this.factory.termToId(object)),
                    quadKey                      = `${subjKey} ${predKey} ${objKey}`,
                    delCount                     = await this.#client.DEL(quadKey);

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

                    quadsDeleted++;
                    this.emit('deleted', quad);
                }
            }));

            return quadsDeleted;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // RedisStore#delete

    async deleteStream(stream) {
        const
            quadStream = await super.deleteStream(stream),
            quadArr    = [];

        await new Promise((resolve) => {
            quadStream.on('data', quad => quadArr.push(quad));
            quadStream.on('end', resolve);
        });

        return this.delete(quadArr);
    } // RedisStore#deleteStream

    async deleteMatches(subject, predicate, object, graph) {
        assert(!graph || this.factory.isDefaultGraph(graph), 'RedisStore#deleteMatches : the graph option is not supported');

        await super.deleteMatches(subject, predicate, object, graph);

        const
            subjKey   = subject ? termIdToKey(this.factory.termToId(subject)) : '?subject',
            predKey   = predicate ? termIdToKey(this.factory.termToId(predicate)) : '?predicate',
            objKey    = object ? termIdToKey(this.factory.termToId(object)) : '?object',
            searchKey = `${subjKey} ${predKey} ${objKey}`;

        try {
            let quadsDeleted = 0;

            const
                quadKeys = (subject && predicate && object)
                    ? [searchKey]
                    : await this.#client.SMEMBERS(searchKey);

            await Promise.all(quadKeys.map(async (quadKey) => {
                try {
                    // TODO delete node
                    // const
                    //     quadData                      = await this.#client.HGETALL(quadKey),
                    //     [subjNode, predNode, objNode] = await Promise.all(
                    //         [quadData.subject, quadData.predicate, quadData.object].map(async (key) => {
                    //             if (termCache.has(key))
                    //                 return termCache.get(key);
                    //             const node = await this.#client.HGETALL(key);
                    //             termCache.set(key, node);
                    //             return node;
                    //         })
                    //     ),
                    //     quad                          = this.factory.fromQuad({
                    //         subject:   subjNode,
                    //         predicate: predNode,
                    //         object:    objNode
                    //     });
                } catch (err) {
                    this.emit('error', err);
                }
            }));

            return quadsDeleted;
        } catch (err) {
            this.emit('error', err);
            throw err;
        }
    } // RedisStore#deleteMatches

    // TODO: has(quads: Quad|Array<Quad>): Promise<boolean>
    // FIXME: on deletion of quads, the terms get left behind, even when no quad refers to them
    // IDEA: tidyUpDatabase(): Promise<number>
    // TODO: validate consistence of operations and check for race conditions

} // RedisStore

module.exports = RedisStore;
