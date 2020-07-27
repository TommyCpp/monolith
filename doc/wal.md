## Write Ahead Log
### Objective
In order to reduce the IO, which is expensive. We should implement an in-memory cache for current opening chunk. And when that chunk get swapped. We can compact it and store it in backend. 

But if the system crushes, we will loss all data in memory. Thus, it's necessary we have a WAL to record every operation we applied to the data store.

### Approaches
1. Cache before Indexer/Storage
```
                         +---------------+
                         |               |
                         |  Indexer      |
                         |               |
                         +-------+-------+
+------------+                   |
|            |                   |
|    WAL     +-------------------+
|            |                   |
+------------+                   |
                         +-------+-------+
                         |               |
                         |  Storage      |
                         |               |
                         +---------------+
```
This is the straight-forward method. 

However, one problem with this problem is that this way WAL will be force to stay in local machine, which could take quite some memory.

2. Cache within Indexer/Storage
```
+------------+ +---------------+
|            | |               |
|    WAL     +-+  Indexer      |
|            | |               |
+------------+ +---------------+

+------------+ +---------------+
|            | |               |
|    WAL     +-+  Storage      |
|            | |               |
+------------+ +---------------+

```
Note that we need to take cautious about how to cache WAL in this case. 

Currently, the chunk will store index information first, and based on the series id returned by `Indexer`. It will processed and store the time point. We will make sure that storing index is always happens before storing time point. And when and only when the index and time point stored in WAL, we will return result to client and change in memory.

1. Crush before storing index. Nothing saved in memory or WAL.
2. Crush after storing index in WAL. But not yet applied to memory. No effect. But when restored, we may find a empty time series.
3. Crush after storing index in WAL and applying to memory. No effect since user will not get any response before `Storage` finishes. Again, we may find an empty time series when restored. 
4. Crush after storing index, and the time point into WAL, but not yet applied to memory. If crushes, we can recover from WAL
5. Crush happens after all operation. We can recover from WAL.

In most of implementation, WAL will have some kind of cache. For example, MongoDB flush wal's content every 100ms. So it's possible to lose the data we haven't flushed to disk yet. 

Here, we must make sure that the index is up to date, that's to say **we must flush for every new time series**. But we can allow caching for time point data. 

The reason is if the index get lost somehow, we may end up lose the status of time series id, which is a increasing series of unsigned 64bit integer. And we may issue the same time series id for different time series. However, if we lose some of the time point, it will not impact the whole system and generally we don't need all time point in time series.