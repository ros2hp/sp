## What is SP?

Scalar Propagation (SP) is the third in the sequence of five programs that constitute the GoGraph RDF load process.  GoGraph is a rudimentary graph database, developed originally in Go principally as a way to learn the language and now refactored in Rust for a similar reason. GoGraph employees the Tokio asynchronous runtime to implement a highly concurrent and asynchronous design. The database design enables scaling to internet size data volumes. GoGraph currently supports AWS's Dynamodb, although other hyper scalable databases, such as Google's Spanner, is also in development. 

## GoGraph RDF Load Components

The table below lists the sequence of programs that load a RDF file into the target database, Dynamodb. There is no limit to the size of the RDF file. 

MySQL is used as an intermediary storage facility providing both query and sort capabilties to each of the load programs. Unlike the Go implementation, the Rust version does not support restartability for any of the load programs. This is left as a future enhancement. It is therefore recommended to take a backup of the database after running each of the load programs. 

| Load Program           |  Repo       |  Task                                                   |  Data Source           | Target Database |
|-----------------------:|-------------|---------------------------------------------------------|------------------------|-----------------|
|  RDF-Loader            |   ldr       | Load a RDF file into Dynamodb and MySQL                 |  RDF file              | Dynamodb, MySQL |
|  Attach                | rust-attach | Link child nodes to parent nodes                        |  MySQL           | Dynamodb        |
|  _Scalar Propagation_  |   _sp_      | _Propagate child scalar data into parent node and generate reverse edge data_      |  _MySQL_        | _Dynamodb_      |
|  Double Propagation    |   dp        | Propagate grandchild scalar data into grandparent node* |  MySQL           | Dynamodb        |
|  ElasticSearch         |   es        | Load data into ElasticSearch                            |  MySQL          | Dynamodb        |


* for 1:1 relationships between grandparent and grandchild nodes

## Why SP? ##

Propagating scalar data replicates the values of all child nodes for a particular parent-child edge to the associated parent node. A query can now be resolved by scanning the replicated data on the parent node instead of probing each of the child nodes indvidually leading to a massive reduction in database requests and  singificantly improving query times.  For example, to determine the average age of all the subscribers for a YouTube content maker would, without any data replication, require the database to query each subscriber. Not an issue if its few tens of subscribers but if its hundreds of thousands then it represents a severe bottleneck on the compute and IO resources. The purpose of scalar propagation in this case is to copy the age of each subscriber into a Columnar like structure associated with the parent. The average age can now be resolved by a single scan operation on the Columnar structure. Parallelising this query is also relatively simple. See GoGraph's Design document (link below) for a detailed description of the "Overflow Blocks" that are used to store the propagated data of each child node. Overflow blocks distributes the replicated data across Dynamodb partitions enabling parallel querying of the data without increasing resource contention. In Dynamodb, each propagated scalar attribute is stored in a LIST data type associated with the parent node - the List type emulates Columnar like storage. Most databases have a LIST type equivalent to varying capabilities. There are performance and cost considerations that impose a limit ont the number of entries that can be stored in an instacne of a LIST type, so in many cases multiple instances (aka "Overflow Batches") of a LIST type may be necessary to store all the propagated attribute data. GoGraph handles the allocation of Overflow batches as required.

Tests have shown that a query involving 1020 child nodes can be performed in 0.122 seconds, substantially faster than querying all 1020 nodes individually.

The downside of all this replication is the cost to maintain data consistency should a child scalar attribute change. To that end GoGraph automatically generates reverse edges for most parent-child edges. A reverse-edge associated with a child node, contains an entry for each parent-child edge the child node is contained in. In some cases it may be just one entry in others it may be thousands. The Overflow Block design used for scalar replication is also used for reverse edges, so it is scalable in both storage and performance.

## GoGraph Design Guide ##

[A detailed description of GoGraph's database design, type system and data model are described in this document](docs/GoGraph-Design-Guide.pdf)

## SP Highlights ##

* Implement a shared cache with LRU eviction policy that supports concurrent operations. A shared cache is of course prone data corruption, deadlacks and race-conditions but extensive testing has shown the implementation to be both robust and safe.
* Configurable number of parallel streams implemented as Tokio Tasks.
* Fully implements Tokio Asynchronous runtime across all operations.

## SP Schematic ##

A simplified view of SP is presented in the two schematics below. The first schematic describes the generation of reverse edge data  (child to parent as opposed to he more usual parent-child) which uses a dedicated cache to aggregate the reverse edges and a LRU algorithm to manage the persistance of data to Dynamodb.  The second schematic shows the simpler scalar propagation load.  Parent-child edges are held in MySQL while the scalar data is queried from Dynamodb for each child node and then saved back into Dynamodb where it is associated with the parent node. No cache is required to propagate the child data.

         ---------------------
         |       MySQL        |
         ---------------------
                   |
                   V

                  Main

                   |
            --------- . . ---      
           |       |         |
           V       V         V

          Load    Load     Load
          Task    Task     Task        (Tokio Tasks asynchronously 
                                        read and write to cache )
          ^  |    ^  |     ^  |         
          |  V    |  V     |  V
      ==============================
      |     Reverse Edge Cache     |     (shared cache)
      ==============================
                ^   |
                |   V

                 LRU                   (responsibile for eviciting cache entries based
                                          on a LRU policy )
                  |                              
                  V

            Persit Service             (Single Task that allocates child tasks to persist data.
                                        Queues tasks if flooded with requests)
          |       |    ..   |             
          V       V         V

        Persist Persist  Persist
         Task    Task     Task

          |       |         |
          V       V         V
          ---------------------
         |     Dynamodb       |
          ---------------------

    Schematic  1  - Aggregating Reverse Edges



           --------------------
          |       MySQL        |
           --------------------
                    |
                    V

                   Main

                    |
            ---------- . . ---       
            |       |  . .   |
            V       V        V

           Load   Load     Load
           Task   Task     Task

           ^  |   ^  |     ^  |        (Read child node scalar data)
           |  V   |  V     |  V       (Write scalar data to parent node)
          ----------------------
         |       Dynamodb       |
          ----------------------

    Schematic 2 - propagating scalar data

## Example of Propagated Scalar Data ##

Propagated scalar data is represented by a single Dynamodb item for each scalar attribute. The PK (Primary Key) value is that of the parent node, as propagated data belongs to the parent node. The SK (Sort Key) value identifies the scalar attribute and the parend-child edge and marks the item as propagated data. The next attribute depends on the type of the propagated attribute. For a String type the attribute is given the name "SL", which stands for "String List" as the LIST data type is used to store the values of each child attribute value belonging to the particular edge. For number types, its "NL" and for boolean types its "BL". The last attribute for the item is "Xf", which represents a flag value and is also a LIST type. There is a 1:1 corespondence between each value in Xf and its associated value in NL or SL or BL. Xf has numerous values but the most important value for our discussion is whether the associated child node has been deleted or not. When a child node is removed its coresponding propagated data is not removed but marked with "soft delete" flag value. 


| PK                             |  SK             |  NL                                          |   Xf                                       | 
|--------------------------------|-----------------|----------------------------------------------|--------------------------------------------|


