# module.persistence.redis

- [Persistence](../module.persistence)

## Interface

```ts
interface RedisStoreFactory extends DataStoreCoreFactory {
    store(graph: NamedNode, client: RedisClient): RedisStore;
};
```