## What is SP?

Scalar Propagation (SP) is the third in the sequence of five components that make up the GoGraph RDF load process.  GoGraph is a rudimentary graph database, developed originally in Go principally as a way to learn the language and now refactored in Rust for the same reason. GoGraph employees the Tokio asynchronous runtime to implement a highly concurrent design. The data model is designed to support internet scale data volumes. It currently supports AWS's Dynamodb, although other hyper scalable databases, such as Google's Spanner, is also envisioned. 

## GoGraph RDF Load Components

The table below lists the sequence of programs that load a RDF file, of any size, into the GoGraph data model in Dynamodb. MySQL is used as an intermediary storage facility providing querying and sorting capabilties for each of the individual load components. Unlike the Go implementation, the Rust version does not support restartable load program. This is left as a future enhancement. It is therefore recommended to take a backup of the database after each of the load programs complete. 

| Load Compoent          |  Repo       |  Task                                                   |  Data Source           | Target Database |
|-----------------------:|-------------|---------------------------------------------------------|------------------------|-----------------|
|  RDF-Loader            |   ldr       | Load RDF file into Dynamodb and MySQL                   |  RDF file              | Dynamodb, MySQL |
|  Attacher              |   attach    | Link child nodes to parent nodes                        |  MySQL           | Dynamodb        |
|  _Scalar Propagation_  |   _sp_      | _Propagate child scalar data into parent node and_      |  _MySQL_        | _Dynamodb_      |
|                        |             | _generate reverse edge data_                            |      | _Dynamodb_      |
|  Double Propagation    |   dp        | Propagate grandchild scalar data into grandparent node* |  MySQL           | Dynamodb        |
|  ElasticSearch         |   es        | Load data into ElasticSearch                            |  MySQL          | Dynamodb        |


* for 1:1 relationships between grandparent and grandchild nodes

## GoGraph Design Guide ##

[A detailed description of GoGraph's database design, type system and data model are described in this document](docs/GoGraph-Design-Guide.pdf)

## SP Highlights ##

* Implement configutable Reverse Edge cache with LRU eviction policy.
* Configurtble number of concurrent Tokio load tasks
* Asynchronous implementation including Dynamodb

## SP Schematic ##

A simplified view of SP is presented in the following two schematics. The first schematic describes the loading of reveerse edge data which uses a dedicated cache to aggregate the reverse edge data, using a LRU algorithm to manage the persistance of data to Dynamodb. On completiong of the load program the cache is flushed to Dynamodb. The Second schematic shows the scalar propagation load where propagating child nodes scalar data into their respective parent node.  scalar data uses MySQL to store the edge data and quries Dynamodb for the child scalar data and store the back into Dynamodb without the requirement cache the data as their is no requirement to aggregate the data. 

         ---------------------
         |       MySQL        |
         ---------------------
                   |
                   V
                  Main
           |       |  . .   |
           V       V        V
          Load    Load     Load
          Task    Task     Task
           |       |        |
           V       V        V
      ==============================
      |     Reverse Edge Cache     |
      ==============================
                  |
                  V
                 LRU
                  |
                  V
            Persit Service
          |       |    ..   |
          V       V         V
        Persist Persist  Persist
         Task    Task     Task
          |       |         |
          V       V         V
          ---------------------
         |     Dynamodb       |
          ---------------------

    Schematic  1  - Agregating Reverse Edges



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

