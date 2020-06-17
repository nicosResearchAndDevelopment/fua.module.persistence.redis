module.exports = ({
    'Helmut': Helmut,
    'redis': redis,
    'hrt': hrt = () => (new Date()).valueOf() / 1000,
    'config': config,
    'default_timeout': default_timeout = 10000 // TODO:config
}) => {

    /**
     * abstract: true
     * @param node
     * @param parameter
     * @returns {{}}
     */
    function module_persistence(node, parameter) {
        let
            module_persistence = {}
            ;
        //module_persistence = rdf:Resource(node, parameter);
        return module_persistence;
    } // function module_persistence ()

    function module_persistence_redis_factory({
        'redis': redis,
        //region interface
        '@id': id = "redis",
        'config': config
    }) {
        const
            client = redis.createClient(config['client'])
            ; // const
        let
            module_persistence_redis = module_persistence({
                '@id': id
            }, /** parameter */ null)
            ; // let

        client['on']("connect", function () {
            console.warn(`module.persistence.redis : on : connect`);
            console.warn(`module.persistence.redis : on : connect : client.server_info.redis_version <${client.server_info.redis_version}>`);
            //client.set("aaa", "b", redis.print); // => "Reply: OK"
            //client.get("aaa", redis.print); // => "Reply: bar"
            //client.quit();
        });
        client['on']("error", function (err) {
            console.warn(`module.persistence.redis : on : error <${err.toString()}>`);
        });

        function module_persistence_redis_CREATE(subject, timeout = default_timeout) {
            return new Promise((resolve, reject) => {
                let semaphore;
                try {
                    semaphore = setTimeout(() => {
                        clearTimeout(semaphore);
                        reject(`agent_persistence_set : timeout <${timeout}> reached.`);
                    }, timeout);
                    module_persistence_redis_READ(subject, '@type').then((result) => {
                        if (semaphore)
                            clearTimeout(semaphore);
                        if (!result) {
                            client['hset'](subject, "@type", "null", (err, res) => {
                                if (err)
                                    reject(err); // TODO: rollback?!?
                                resolve("created");
                            });
                        } else {
                            reject(`module_persistence_redis_CREATE : subject <${subject}> already presen.`);
                        } // if ()
                    }).catch((err) => {
                        if (semaphore)
                            clearTimeout(semaphore);
                        reject(err);
                    });
                } catch (jex) {
                    if (semaphore)
                        clearTimeout(semaphore);
                    reject(jex);
                } // try
            }); // return new P
        } // function module_persistence_redis_CREATE()

        function module_persistence_redis_READ(subject, key, timeout = default_timeout) {
            return new Promise((resolve, reject) => {
                let semaphore;
                try {
                    semaphore = setTimeout(() => {
                        clearTimeout(semaphore);
                        reject(`agent_persistence_set : timeout <${timeout}> reached.`);
                    }, timeout);
                    if (key) {
                        let array_key = Array.isArray(key);
                        key = ((array_key) ? key : [key]);
                        Promise.all(key.map((k) => {
                            return new Promise((resolve, reject) => {
                                client['hget'](subject, k, (err, reply) => {
                                    if (err)
                                        reject(err); // TODO: rollback?!?
                                    resolve(((reply) ? JSON.parse(reply) : undefined));
                                });
                            });
                        })).then((results) => {
                            if (semaphore)
                                clearTimeout(semaphore);
                            resolve(((array_key) ? results : results[0]));
                        }).catch((err) => {
                            if (semaphore)
                                clearTimeout(semaphore);
                            reject(err);
                        });
                    } else {
                        client['hgetall'](subject, (err, reply) => {
                            let node = {};
                            if (semaphore)
                                clearTimeout(semaphore);
                            if (err)
                                reject(err)
                            for (let predicate in reply) {
                                // TODO: eleminate 'if ((predicate !== "null") && (predicate !== "undefined"))'
                                if ((predicate !== "null") && (predicate !== "undefined"))
                                    node[predicate] = JSON.parse(reply[predicate]);
                            } // for()
                            node['@id'] = subject;
                            resolve(node);
                        });
                    } // if ()
                } catch (jex) {
                    if (semaphore)
                        clearTimeout(semaphore);
                    reject(jex);
                } // try
            }); // return new P
        } // function module_persistence_redis_READ()

        function module_persistence_redis_UPDATE(subject, key, value, timeout = default_timeout) {
            return new Promise((resolve, reject) => {
                let semaphore;
                try {
                    semaphore = setTimeout(() => {
                        clearTimeout(semaphore);
                        reject(`agent_persistence_set : timeout <${timeout}> reached.`);
                    }, timeout);
                    client['hset'](subject, key, JSON.stringify(value), (err, res) => {
                        //client['hset'](subject, key, value, (err, res) => {
                        if (semaphore)
                            clearTimeout(semaphore);
                        if (err)
                            reject(err); // TODO: rollback?!?
                        resolve("updated");
                    });
                } catch (jex) {
                    if (semaphore)
                        clearTimeout(semaphore);
                    reject(jex);
                } // try
            }); // return new P
        } // function module_persistence_redis_UPDATE()

        function module_persistence_redis_DELETE(subject, predicate, timeout = default_timeout) {
            return new Promise((resolve, reject) => {
                let semaphore;
                try {
                    semaphore = setTimeout(() => {
                        clearTimeout(semaphore);
                        reject(`agent_persistence_set : timeout <${timeout}> reached.`);
                    }, timeout);

                    if (!predicate) {
                        client['del'](subject, (err, res) => {
                            //client['hset'](subject, key, value, (err, res) => {
                            if (semaphore)
                                clearTimeout(semaphore);
                            if (err)
                                reject(err); // TODO: rollback?!?
                            resolve("deleted");
                        });
                    } else {
                        throw (new Error());
                    } // if ()
                } catch (jex) {
                    if (semaphore)
                        clearTimeout(semaphore);
                    reject(jex);
                } // try
            }); // return new P
        } // function module_persistence_redis_DELETE()

        function module_persistence_redis_LIST(subject, predicate, timeout = default_timeout) {
            return new Promise((resolve, reject) => {
                let semaphore;
                try {
                    semaphore = setTimeout(() => {
                        clearTimeout(semaphore);
                        reject(`agent_persistence_set : timeout <${timeout}> reached.`);
                    }, timeout);

                    //if (!predicate) {
                    //    client['del'](subject, (err, res) => {
                    //        //client['hset'](subject, key, value, (err, res) => {
                    //        if (semaphore)
                    //            clearTimeout(semaphore);
                    //        if (err)
                    //            reject(err); // TODO: rollback?!?
                    //        resolve("deleted");
                    //    });
                    //} else {
                    //    throw(new Error());
                    //} // if ()
                    reject(new Error("module.persistence.redis : LIST not implemented"));
                } catch (jex) {
                    if (semaphore)
                        clearTimeout(semaphore);
                    reject(jex);
                } // try
            }); // return new P
        } // function module_persistence_redis_LIST()

        function module_persistence_redis_set(node, hash_id = true, encrypt_value = true, timeout = default_timeout) {
            let array_request = Array.isArray(node);
            node = ((Array.isArray(node)) ? node : [node]);
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
                                let hash = ((hash_id) ? Helmut.hash({ 'value': n['@id'] }) : n['@id']);
                                client.set(hash, ((encrypt_value) ? Helmut.encrypt(JSON.stringify(n)) : JSON.stringify(n)), (err, result) => {
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
            id = ((array_request) ? id : [id]);
            return new Promise((resolve, reject) => {
                try {
                    Promise.all(id.map((_id) => {
                        return new Promise((_resolve, _reject) => {
                            client.get(((hash_id) ? Helmut.hash({ 'value': _id }) : _id), function (err, reply) {
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

        Object.defineProperties(module_persistence_redis, {
            '@id': { value: id },
            '@type': { value: ["fua:PersistantAdapterRedis", "fua:PersistantAdapter", "rdf:Resource"] },
            'mode': { value: "redis" }
            ,
            //REM: interface methods are shoen with capital letters
            'CREATE': { value: module_persistence_redis_CREATE },
            'UPDATE': { value: module_persistence_redis_UPDATE },
            'READ': { value: module_persistence_redis_READ },
            'DELETE': { value: module_persistence_redis_DELETE },
            'LIST': { value: module_persistence_redis_LIST }
            ,
            'set': { value: module_persistence_redis_set },
            'get': { value: module_persistence_redis_get }
        }); // Object.defineProperties()

        return module_persistence_redis;

    } // module_persistence_redis_factory ()

    return module_persistence_redis_factory({ 'redis': redis, 'hrt': hrt, 'config': config });

};
