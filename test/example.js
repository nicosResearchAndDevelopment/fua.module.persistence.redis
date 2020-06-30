const
    redis = require("redis"),
    redis_client = redis.createClient(),
    promify = (fn, ...args) => new Promise((resolve, reject) => fn(...args, (err, result) => err ? reject(err) : resolve(result))),
    module_persistence_redis = require("../src/module.persistence.redis.js");

// REM Only ever run on a test database. Queries might destroy active data.
// I would advice to download redis stable from https://redis.io/download/  and run it locally.

(async (/* async IIFE */) => {

    await promify(redis_client.FLUSHALL.bind(redis_client));

    const redis_persistence_adapter = module_persistence_redis({
        'redis': redis,
        'client': redis_client,
        'config': {
            'client': undefined
        },
        // 'default_timeout': 10e3
    });

    await redis_persistence_adapter.CREATE("test:hello_world");
    await redis_persistence_adapter.UPDATE("test:hello_world", "@type", ["rdfs:Resource", "ldp:NonRDFSource", "xsd:string"]);
    await redis_persistence_adapter.UPDATE("test:hello_world", "@value", "Hello World!");
    await redis_persistence_adapter.CREATE("test:lorem_ipsum");
    await redis_persistence_adapter.UPDATE("test:lorem_ipsum", "rdf:label", "Lorem Ipsum");
    await redis_persistence_adapter.UPDATE("test:lorem_ipsum", "test:property", "test:hello_world");
    await redis_persistence_adapter.UPDATE("test:hello_world", "test:marzipan", "test:lorem_ipsum");
    console.log("READ(test:hello_world) =>", await redis_persistence_adapter.READ("test:hello_world"), "\n");
    console.log("LIST(test:lorem_ipsum, test:property) =>", await redis_persistence_adapter.LIST("test:lorem_ipsum", "test:property"), "\n");
    await redis_persistence_adapter.DELETE("test:hello_world", "test:marzipan", "test:lorem_ipsum");

})(/* async IIFE */).catch(console.error);
