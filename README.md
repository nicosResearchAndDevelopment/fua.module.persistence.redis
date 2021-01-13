# module.persistence.redis

## Interface

```ts
interface RedisStoreFactory extends DataStoreCoreFactory {
    store(graph: NamedNode, client: RedisClient): RedisStore;
};
```