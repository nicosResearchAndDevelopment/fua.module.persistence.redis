# module.persistence.redis

- [Persistence](https://git02.int.nsc.ag/Research/fua/lib/module.persistence)

## Interface

```ts
interface RedisStoreFactory extends DataStoreCoreFactory {
    store(graph: NamedNode, client: RedisClient): RedisStore;
};
```