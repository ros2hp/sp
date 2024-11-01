## What is SP?

Scalar Propagation (SP) is the third in the sequence of five components that make up the GoGraph RDF load process.  GoGraph is a rudimentary graph database, developed originally in Go principally as a way to learn the language and now refactored in Rust for the same reason. GoGraph employees the Tokio asynchronous runtime to implement a highly concurrent design. The data model is designed to support internet scale data volumes. It currently supports AWS's Dynamodb, although other hyper scalable databases, such as Google's Spanner, is also envisioned. 

## GoGraph RDF Load Components

The table below lists the sequence of programs that load a RDF file, of any size, into the GoGraph data model in Dynamodb. MySQL is used as an intermediary storage facility providing querying and sorting capabilties for each of the individual load components. Unlike the Go implementation, the Rust version does not support restartable load program. This is left as a future enhancement. It is therefore recommended to take a backup of the database after each of the load programs complete. 

| Load Compoent          |  Repo       |  Task                                                   |  Data Source           | Target Database |
|-----------------------:|-------------|---------------------------------------------------------|------------------------|-----------------|
|  RDF-Loader            |  ldr        | Load RDF file into Dynamodb and MySQL                   |  RDF file              | Dynamodb, MySQL |
|  Attacher              | rust/attach | Link child nodes to parent nodes                        |  MySQL           | Dynamodb        |
|  _Scalar Propagation_  |   _sp_      | _Propagate child scalar data into parent node and_      |  _MySQL_        | _Dynamodb_      |
|                        |             | _generate reverse edge data_                            |      | _Dynamodb_      |
|  Double Propagation    |   dp        | Propagate grandchild scalar data into grandparent node* |  MySQL           | Dynamodb        |
|  ElasticSearch         |   es        | Load data into ElasticSearch                            |  MySQL          | Dynamodb        |


* for 1:1 relationships between grandparent and grandchild nodes

## Why SP? ##

Significantly improved query performance.  Replicating the scalar data of all child nodes into the parent node completely eliminates the database requests on the individual child nodes for any query on a node involving its children.  For a node that has hundreds of thousands of child nodes this effectively means hundreds of thousands of databses requests are eliminated for queries involving any of the child nodes attributes. See GoGraph's Design document (link below) for a description of how the propagated data is stored in Overflow Blocks associated with the parent node. Overflow blocks distributes the data across Dynamodb servers and enables parallel querying of the propagated data without increasing resource contention. In addition the propagated scalar data is stored in Dynamodb's LIST data types (or the equivalent in whatever database is used) which effiectively represents a Columnar type, commonly used in data warehouse sollutions, enabling a query of the child data to be implemented as a simple scan operation on the LIST type.

The downside of course is ...

## GoGraph Design Guide ##

[A detailed description of GoGraph's database design, type system and data model are described in this document](docs/GoGraph-Design-Guide.pdf)

## SP Highlights ##

* Implement a cache with LRU eviction policy.
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
           |       |  . .   |
           V       V        V
          Load    Load     Load
          Task    Task     Task        (Tokio Tasks concurrently and asynchronously 
          ^  |    ^  |     ^  |             read and write to cache )
          |  V    |  V     |  V
      ==============================
      |     Reverse Edge Cache     |      (configurable cache size)
      ==============================
                  |
                  V
                 LRU                   (responsibile for eviciting cache entries based
                  |                              on LRU policy )
                  V
            Persit Service             (Single Task that allocates child tasks to persist data.
          |       |    ..   |             Queues tasks if flooded with requests)
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
            |       |  . .   |
            V       V        V
           Load   Load     Load
           Task   Task     Task
           ^  |   ^  |     ^  |      (Read child node scalar data)
           |  V   |  V     |  V    (Write scalar data to parent node)
          ----------------------
         |       Dynamodb       |
          ----------------------

    Schematic 2 - propagating scalar data

## Example of Propagated Data in Dynamodb ##