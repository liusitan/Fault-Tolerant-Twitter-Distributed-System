Our general approach to fault-tolerance and consistency to lab3 follows the principle of the separation of concern. Our frontend remains the same as in lab2. We designed keepers and bin clients in a different way such that fault tolerance is possible. At the very center of our design is consistent hashing. We borrowed this idea from Chord [1]: we map identifiers of different components to a “ring” (using a pre-determined consistent hashing function). In this way, we construct a mapping between different components in the system and bring only minimum overhead to achieve fault tolerance.

## 1.Bin Client

When bin_new_client is called, it returns BinStorage,and the BinStorage can generate the Bin Client and pass it the bin name and the address list. And the bin-client will translate them into its position in the hash ring. Bin Client is still stateless this time. For each modification operation, either it successfully appends to the backend with a unique timestamp or fails, and the modification operation only succeeds if the bin client has written to the two backends(the active two that are closest to their original position in the hash ring). For each query operation, it decides the most recent value based on information retrieved from two backends.

## 2.Keeper

When serve_keeper() is called, besides meeting lab 2’s requirement (e.g., ready signal, shutdown signal), the keeper first creates a new thread to serve a RPC service using keeper.proto, so other keepers can communicate with this service and see if this keeper is still alive. Every second, the keeper checks if the shutdown signal has arrived, and calls heart_beat() if not.

heart_beat() is where we monitor the status of other keepers, backends, and handle crashed or newly added nodes. 

All keepers and backends are put on the circle mentioned in Chord [1] using a consistent hash. A keeper is **responsible_for** all backends that are between itself and its previous alive keeper on the circle, and **sync_clock_for** these backends and two nearby backends. It records whether a backend in the **responsible_for** list is alive or dead.

In heart_beat(), a keeper first synchronizes the clocks of backends in the **sync_clock_for** list. During this, the keeper may notice some backend in its **responsible_for** list is now alive but it was dead in the last heart_beat(), and then it will handle the join of the backend. It may also notice some backend in its **responsible_for** list is now dead but it was alive in the last heart_beat(), and then it will handle the crash of the backend. These two cases are discussed in  **3.1**.

The keeper will also check whether its previous keeper is dead, and any keeper between itself and the previous keeper is alive (added). See more details in **3.2**

## 3.Fault tolerance

Our system is designed to be fault tolerant. To be specific, clients (and frontend) always remain stateless. The keepers' and backends’ fault tolerance are collaboratively achieved using primary-backup copies and write-ahead logs.

### 3.1 Fault Tolerance of backend

The Fault Tolerance of the backend is achieved both by the bin-client operation and keeper migration behaviors. First, each data is ensured to be written to two backends by the bin client. In this way, if either one of the  backends fails, the keeper will detect it and migrate the data. As for the client, it changes its two backends, and continues what it usually does. For a bin client, each call to the bin client has to combine the info from the two servers, so the failure of a single server would  not affect the consistency of the bin-clients interface semantics. If a node dies, another node will be chosen by the bin-client,and the query operation will combine the both of them to get the current state, since one of them must have all the logs. So fault tolerance is achieved. 

Keepers are responsible for migrating data from one server to another for fault tolerance. To be more specific, each keeper is assigned a list of backends that it should check every second. Once it detects a backend failure, it will move data accordingly to bring the system back into a consistent state. Keepers also use write-ahead logs such that one keeper task could be handed over to others during keeper failures.

### 3.2 Fault Tolerance of keeper

Each keeper keeps track of its previous keepers on the consistent hash ring. Every second, each living keeper sends a heartbeat to its previous keeper and is responsible for taking over the previous keeper’s unfinished migration task should the previous keeper die. Moreover, it scans every offline keeper with a hash value in between and in this way, it could detect any new keeper that wants to join the system, and update related data like its **responsible_for** list.

## 4.consistent hash
Each ip address of keeper and the backends will be hashed by rust’s default hasher by the “http://” + ip address(ex: “http://0.0.0.0:2322”), and each bin name will be hashed by the “bin#biname”. In this way they are all mapped to the u64 ring. For each bin’s position on the ring, we use binary search to determine. 

Reference

[1] ​​Ion Stoica, Robert Morris, David Liben-Nowell, David R. Karger, M. Frans Kaashoek, Frank Dabek, and Hari Balakrishnan. 2003. Chord: a scalable peer-to-peer lookup protocol for internet applications. IEEE/ACM Trans. Netw. 11, 1 (February 2003), 17–32. https://doi.org/10.1109/TNET.2002.808407
