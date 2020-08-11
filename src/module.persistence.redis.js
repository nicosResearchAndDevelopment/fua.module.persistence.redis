const
    regex_semantic_id     = /^https?:\/\/\S+$|^\w+:\S+$/,
    regex_nonempty_key    = /\S/,
    array_primitive_types = Object.freeze(["boolean", "number", "string"])
;

/**
 * @param {*} value
 * @param {String} errMsg
 * @param {Class<Error>} [errType=Error]
 * @throws {Error<errType>} if the value is falsy
 */
function assert(value, errMsg, errType = Error) {
    if (!value) {
        const err = new errType(`redis_adapter : ${errMsg}`);
        Error.captureStackTrace(err, assert);
        throw err;
    }
} // assert

/**
 * Returns true, if the value does include at least one nonspace character.
 * @param {String} value
 * @returns {Boolean}
 */
function is_nonempty_key(value) {
    return regex_nonempty_key.test(value);
} // is_nonempty_key

/**
 * This is an IRI or a prefixed IRI.
 * @typedef {String|IRI} SemanticID
 *
 * Returns true, if the value is a complete or prefixed IRI.
 * This function is important to distinct values from IRIs and
 * to make sure, subject, predicate and object have valid ids.
 * @param {SemanticID} value
 * @returns {Boolean}
 */
function is_semantic_id(value) {
    return regex_semantic_id.test(value);
} // is_semantic_id

/**
 * This are the only values neo4j can store on a node.
 * @typedef {null|Boolean|Number|String|Array<Boolean>|Array<Number>|Array<String>} PrimitiveValue
 *
 * Returns true, if the value is primitive. This function
 * is important to make sure, a value can be stored in neo4j.
 * @param {PrimitiveValue} value
 * @returns {Boolean}
 */
function is_primitive_value(value) {
    return value === null
        || array_primitive_types.includes(typeof value)
        || (Array.isArray(value) && array_primitive_types.some(
            type => value.every(arrValue => typeof arrValue === type)
        ));
} // is_primitive_value

/**
 * This is the general concept of a persistence adapter.
 * @typedef {Object} PersistenceAdapter
 * @property {Function} CREATE Create a resource.
 * @property {Function} READ Return a resource or some properties.
 * @property {Function} UPDATE Update a property or a reference.
 * @property {Function} DELETE Delete a resource or a reference.
 * @property {Function} LIST List targets of a reference on a resource.
 *
 * This is a persistent adapter with build in methods for redis.
 * @typedef {PersistenceAdapter} RedisAdapter
 *
 * This is the factory method to build a persistence adapter for redis.
 * @param {Object} config
 * @param {Redis~Client} config.client
 * @returns {RedisAdapter}
 */
module.exports = function (config) {

    assert(typeof config === "object" && config !== null,
        "The config for a persistence adapter must be a nonnull object.");
    assert(typeof config["client"] === "object" && config["client"] !== null && typeof config["client"]["HGET"] === "function",
        "The config.client must contain a redis client instance.");

    /** @type {Redis~Client} */
    const
        redis_client = config["client"],
        Helmut       = config['Helmut']
    ;

    /**
     * Uses the redis client to make a method call and returns
     * the result as promise instead of using callbacks.
     * @async
     * @param {string} method
     * @param  {...*} args
     * @returns {*}
     */
    async function request_redis(method, ...args) {
        return new Promise((resolve, reject) => {
            redis_client[method](...args, (err, result) => {
                if (err) reject(err);
                else resolve(result);
            });
        });
    } // request_redis

    /**
     * TODO describe operation EXIST
     * @async
     * @param {SemanticID} subject
     * @returns {Boolean}
     */
    async function operation_redis_exist(subject) {

        assert(is_semantic_id(subject),
            `operation_exist : invalid {SemanticID} subject <${subject}>`);

        /** @type {Number} */
        const existsRecord = await request_redis("EXISTS", subject);
        return !!existsRecord;

    } // operation_redis_exist

    /**
     * TODO describe operation CREATE
     * @async
     * @param {SemanticID} subject
     * @returns {Boolean}
     */
    async function operation_redis_create(subject) {

        assert(is_semantic_id(subject),
            `operation_create : invalid {SemanticID} subject <${subject}>`);

        /** @type {Number} */
        const createRecord = await request_redis("HSETNX", subject, "@id", JSON.stringify(subject));
        if (createRecord) await request_redis("HSETNX", subject, "@type", JSON.stringify(["rdfs:Resource"]));
        return !!createRecord;

    } // operation_redis_create

    /**
     * TODO describe operation READ_subject
     * @async
     * @param {SemanticID} subject
     * @returns {Object|null}
     */
    async function operation_redis_read_subject(subject) {

        assert(is_semantic_id(subject),
            `operation_read_subject : invalid {SemanticID} subject <${subject}>`);

        /** @type {Object<String>|null} */
        const readRecord = await request_redis("HGETALL", subject);
        if (readRecord) for (let key in readRecord) {
            readRecord[key] = JSON.parse(readRecord[key]);
        } // if-for

        return readRecord;

    } // operation_redis_read_subject

    /**
     * TODO describe operation READ_type
     * @async
     * @param {SemanticID} subject
     * @returns {Array<SemanticID>}
     */
    async function operation_redis_read_type(subject) {

        assert(is_semantic_id(subject),
            `operation_read_type : invalid {SemanticID} subject <${subject}>`);

        /** @type {String|null} */
        const readRecord = await request_redis("HGET", subject, "@type");
        return readRecord ? JSON.parse(readRecord) : null;

    } // operation_redis_read_type

    /**
     * TODO describe operation READ
     * @async
     * @param {SemanticID} subject
     * @param {String|Array<String>} [key]
     * @returns {Object|null|PrimitiveValue|Array<PrimitiveValue>}
     */
    async function operation_redis_read(subject, key) {

        if (!key) return await operation_redis_read_subject(subject);
        if (key === "@type") return await operation_redis_read_type(subject);

        assert(is_semantic_id(subject),
            `operation_read : invalid {SemanticID} subject <${subject}>`);

        const isArray = Array.isArray(key);
        /** @type {Array<String>} */
        const keyArr  = isArray ? key : [key];

        assert(keyArr.every(is_nonempty_key),
            `operation_read : {String|Array<String>} ${isArray ? "some " : ""}key <${key}> is empty`);

        if (!(await operation_redis_exist(subject)))
            return null;

        /** @type {Array<String|null>} */
        const readRecords = await request_redis("HMGET", subject, ...keyArr);
        const valueArr    = readRecords.map(val => val ? JSON.parse(val) : null);

        return isArray ? valueArr : valueArr[0];

    } // operation_redis_read

    /**
     * TODO describe operation UPDATE_predicate
     * @async
     * @param {SemanticID} subject
     * @param {SemanticID} predicate
     * @param {SemanticID} object
     * @returns {Boolean}
     */
    async function operation_redis_update_predicate(subject, predicate, object) {

        assert(is_semantic_id(subject),
            `operation_update_predicate : invalid {SemanticID} subject <${subject}>`);
        assert(is_semantic_id(predicate),
            `operation_update_predicate : invalid {SemanticID} predicate <${predicate}>`);
        assert(is_semantic_id(object),
            `operation_update_predicate : invalid {SemanticID} object <${object}>`);

        if (!(await operation_redis_exist(subject)))
            return false;

        /** @type {Array<SemanticID>|null} */
        const
            prevObjects = await operation_redis_list(subject, predicate),
            nextObjects = prevObjects
                ? prevObjects.includes(object)
                    ? null
                    : [...prevObjects, object]
                : [object];

        if (nextObjects)
            await request_redis("HSET", subject, predicate, JSON.stringify(nextObjects));
        return true;

    } // operation_redis_update_predicate

    /**
     * TODO describe operation UPDATE_type
     * @async
     * @param {SemanticID} subject
     * @param {SemanticID|Array<SemanticID>} type
     * @returns {Boolean}
     */
    async function operation_redis_update_type(subject, type) {

        assert(is_semantic_id(subject),
            `operation_update_type : invalid {SemanticID} subject <${subject}>`);

        /** @type {Array<SemanticID>} */
        const typeArr = Array.isArray(type) ? type : [type];

        assert(typeArr.every(is_semantic_id),
            `operation_update_type : invalid {SemanticID|Array<SemanticID>} type <${type}>`);
        if (!typeArr.includes("rdfs:Resource"))
            typeArr.push("rdfs:Resource");

        /** @type {Array<SemanticID>} */
        const prevTypes = await operation_redis_read_type(subject);
        if (!prevTypes) return false;

        if (typeArr.some(val => !prevTypes.includes(val)) || prevTypes.some(val => !typeArr.includes(val)))
            await request_redis("HSET", subject, "@type", JSON.stringify(typeArr));
        return true;

    } // operation_redis_update_type

    /**
     * TODO describe operation UPDATE
     * @async
     * @param {SemanticID} subject
     * @param {String|SemanticID} key
     * @param {PrimitiveValue|SemanticID} value
     * @returns {Boolean}
     */
    async function operation_redis_update(subject, key, value) {

        if (key === "@type") return await operation_redis_update_type(subject, value);
        if (is_semantic_id(key) && is_semantic_id(value)) return await operation_redis_update_predicate(subject, key, value);

        assert(is_semantic_id(subject),
            `operation_update : invalid {SemanticID} subject <${subject}>`);
        assert(is_nonempty_key(key),
            `operation_update : {String|SemanticID} key <${key}> is empty`);
        assert(is_primitive_value(value),
            `operation_update : invalid {PrimitiveValue|SemanticID} value <${value}>`);

        if (!(await operation_redis_exist(subject)))
            return false;

        await request_redis("HSET", subject, key, JSON.stringify(value));
        return true;

    } // operation_redis_update

    /**
     * TODO describe operation DELETE_predicate
     * @async
     * @param {SemanticID} subject
     * @param {SemanticID} predicate
     * @param {SemanticID} object
     * @returns {Boolean}
     */
    async function operation_redis_delete_predicate(subject, predicate, object) {

        assert(is_semantic_id(subject),
            `operation_delete_predicate : invalid {SemanticID} subject <${subject}>`);
        assert(is_semantic_id(predicate),
            `operation_delete_predicate : invalid {SemanticID} predicate <${predicate}>`);
        assert(is_semantic_id(object),
            `operation_delete_predicate : invalid {SemanticID} object <${object}>`);

        /** @type {Array<SemanticID>|null} */
        const prevObjects = await operation_redis_list(subject, predicate);
        if (!prevObjects) return false;
        const objIndex = prevObjects.indexOf(object);
        if (objIndex < 0) return false;

        /** @type {Array<SemanticID>} */
        const nextObjects = prevObjects;
        nextObjects.splice(objIndex, 1);
        if (nextObjects.length > 0) await request_redis("HSET", subject, predicate, JSON.stringify(nextObjects));
        else await request_redis("HDEL", subject, predicate);
        return true;

    } // operation_redis_delete_predicate

    /**
     * TODO describe operation DELETE
     * @async
     * @param {SemanticID} subject
     * @param {SemanticID} [predicate]
     * @param {SemanticID} [object]
     * @returns {Boolean}
     */
    async function operation_redis_delete(subject, predicate, object) {

        if (predicate || object) return await operation_redis_delete_predicate(subject, predicate, object);

        assert(is_semantic_id(subject),
            `operation_delete : invalid {SemanticID} subject <${subject}>`);

        const deleteRecord = await request_redis("DEL", subject);
        return !!deleteRecord;

    } // operation_redis_delete

    /**
     * TODO describe operation LIST
     * @async
     * @param {SemanticID} subject
     * @param {SemanticID} predicate
     * @returns {Array<SemanticID>|null}
     */
    async function operation_redis_list(subject, predicate) {

        assert(is_semantic_id(subject),
            `operation_list : invalid {SemanticID} subject <${subject}>`);
        assert(is_semantic_id(predicate),
            `operation_list : invalid {SemanticID} predicate <${predicate}>`);

        const listRecord = await request_redis("HGET", subject, predicate);
        return listRecord ? JSON.parse(listRecord) : null;

    } // operation_redis_list ()

    /**
     * Creates a promise that times out after a given number of seconds.
     * If the original promise finishes before that, the error or result
     * will be resolved or rejected accordingly and the timeout will be canceled.
     * @param {Promise} origPromise
     * @param {Number} timeoutDelay
     * @param {String} [errMsg="This promise timed out after waiting ${timeoutDelay}s for the original promise."]
     * @returns {Promise}
     */
    function create_timeout_promise(origPromise, timeoutDelay, errMsg) {
        assert(origPromise instanceof Promise,
            "The promise must be a Promise.");
        assert(typeof timeoutDelay === "number" && timeoutDelay > 0,
            "The timeout must be a number greater than 0.");

        let timeoutErr = new Error(typeof errMsg === "string" ? errMsg :
            `This promise timed out after waiting ${timeoutDelay}s for the original promise.`);
        Object.defineProperty(timeoutErr, "name", {value: "TimeoutError"});
        Error.captureStackTrace(timeoutErr, create_timeout_promise);

        return new Promise((resolve, reject) => {
            let
                pending   = true,
                timeoutID = setTimeout(() => {
                    if (pending) {
                        pending = false;
                        clearTimeout(timeoutID);
                        reject(timeoutErr);
                    }
                }, 1e3 * timeoutDelay);

            origPromise.then((result) => {
                if (pending) {
                    pending = false;
                    clearTimeout(timeoutID);
                    resolve(result);
                }
            }).catch((err) => {
                if (pending) {
                    pending = false;
                    clearTimeout(timeoutID);
                    reject(err);
                }
            });
        });
    } // create_timeout_promise

    //region non-interface methods
    function module_persistence_redis_set(node, hash_id = true, encrypt_value = true, timeout = /** default_timeout */ 5000) {
        let array_request = Array.isArray(node);
        node              = ((Array.isArray(node)) ? node : [node]);
        return new Promise((resolve, reject) => {

            let semaphore;

            try {
                semaphore = setTimeout(() => {
                    clearTimeout(semaphore);
                    reject(`agent_persistence_set : timeout <${timeout}> reached.`);
                }, timeout);

                try {
                    Promise.all(node.map((n) => {
                        //return () => {
                        return new Promise((_resolve, _reject) => {
                            if (!n['@id'])
                                _reject(new Error(`node misses '@id'`));
                            let hash = ((hash_id) ? Helmut.hash({'value': n['@id']}) : n['@id']);
                            redis_client.set(hash, ((encrypt_value) ? Helmut.encrypt(JSON.stringify(n)) : JSON.stringify(n)), (err, result) => {
                                if (err)
                                    reject(err);
                                _resolve(n['@id']);
                            });
                        }); // return
                    })).then((result) => {
                        if (array_request)
                            resolve(result);
                        resolve(result[0]);
                    }).catch(reject); // Promise.all()()
                } catch (jex) {
                    reject(jex);
                } // try
            } catch (jex) {
                if (semaphore)
                    clearTimeout(semaphore);
                reject(jex);
            } // try
        }); // return new P
    } // function module_persistence_redis_set()

    function module_persistence_redis_get(id, hash_id = true, decrypt_value = true) {
        let array_request = Array.isArray(id);
        id                = ((array_request) ? id : [id]);
        return new Promise((resolve, reject) => {
            try {
                Promise.all(id.map((_id) => {
                    return new Promise((_resolve, _reject) => {
                        redis_client.get(((hash_id) ? Helmut.hash({'value': _id}) : _id), function (err, reply) {
                            if (err)
                                reject(err)
                            if (decrypt_value) {
                                Helmut.decrypt(reply).then((value) => {
                                    _resolve(JSON.parse(value));
                                }).catch(reject);
                            } else {
                                _resolve(JSON.parse(reply));
                            } // if ()
                        });
                    }); // return
                })).then((result) => {
                    if (array_request)
                        resolve(result);
                    resolve(result[0]);
                }).catch(reject); // Promise.all()
            } catch (jex) {
                reject(jex);
            } // try
        }); // return P
    } // function module_persistence_redis_get()
    //endregion non-interface methods

    /** @type {RedisAdapter} */
        //const redis_adapter = Object.freeze({
        //    "CREATE": (subject, timeout) => !timeout ? operation_redis_create(subject) : create_timeout_promise(operation_redis_create(subject), timeout),
        //    "READ": (subject, key, timeout) => !timeout ? operation_redis_read(subject, key) : create_timeout_promise(operation_redis_read(subject, key), timeout),
        //    "UPDATE": (subject, key, value, timeout) => !timeout ? operation_redis_update(subject, key, value) : create_timeout_promise(operation_redis_update(subject, key, value), timeout),
        //    "DELETE": (subject, predicate, object, timeout) => !timeout ? operation_redis_delete(subject, predicate, object) : create_timeout_promise(operation_redis_delete(subject, predicate, object), timeout),
        //    "LIST": (subject, predicate, timeout) => !timeout ? operation_redis_list(subject, predicate) : create_timeout_promise(operation_redis_list(subject, predicate), timeout)
        //    , // REM : non-interface mthodes
        //    'set':  {value: module_persistence_redis_set},
        //    'get':  {value: module_persistence_redis_get}
        //}); // redis_adapter

    let redis_adapter = {};

    Object.defineProperties(redis_adapter, {
        '@id':    {value: "redis"},
        '@type':  {value: ["fua:PersistantAdapterRedis", "fua:PersistantAdapter", "rdf:Resource"]},
        'mode':   {value: "redis"}
        ,
        // REM: interface methods are shoen with capital letters
        'CREATE': {value: (subject, timeout) => !timeout ? operation_redis_create(subject) : create_timeout_promise(operation_redis_create(subject), timeout)},
        'UPDATE': {value: (subject, key, value, timeout) => !timeout ? operation_redis_update(subject, key, value) : create_timeout_promise(operation_redis_update(subject, key, value), timeout)},
        'READ':   {value: (subject, key, timeout) => !timeout ? operation_redis_read(subject, key) : create_timeout_promise(operation_redis_read(subject, key), timeout)},
        'DELETE': {value: (subject, predicate, object, timeout) => !timeout ? operation_redis_delete(subject, predicate, object) : create_timeout_promise(operation_redis_delete(subject, predicate, object), timeout)},
        'LIST':   {value: (subject, predicate, timeout) => !timeout ? operation_redis_list(subject, predicate) : create_timeout_promise(operation_redis_list(subject, predicate), timeout)}
        ,
        'set':    {value: module_persistence_redis_set},
        'get':    {value: module_persistence_redis_get}
    }); // Object.defineProperties()

    return redis_adapter;

}; // module.exports