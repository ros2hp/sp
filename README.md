## What is SP?

Scalar Propagation (SP) is the third in the sequence of five components that make up the GoGraph RDF load process. GoGraph is a rudimentary graph database, developed originally in Go principally as a way to learn the language and now refactored in Rust for the same reason. GoGraph is a highly concurrent design making use of the Tokio crate in the case of Rust, to implement a highly asynchronous design. It is designed to support internet scale data volumes and concurrent access. It currently supports AWS's Dynamodb, although other hyper scalable databases, such as Google's Spanner, is also envisioned. 

## GoGraph Load Components

The table below lists the order of the programs that process a RDF file into the GoGraph data model in Dynamodb. There is no size limit to the RDF file. MySQL is used as an intermediary storage facility providing querying and sorting capabilties by each of the load programs. In the case of the Rust implemenation, none of the load programs are restartable should en error occur.  This is left as a future enhancement. The Go implementation of GoGraph are all restartable.

| Load Compoent          | Binary      |  Task                                                   |  Data Source           | Target Database |
|-----------------------:|-------------|---------------------------------------------------------|------------------------|-----------------|
|  RDF-Loader            |   ldr       | Load RDF file into Dynamodb and MySQL                   |  RDF file              | Dynamodb, MySQL |
|  Attacher              |   attach    | Link child nodes to parent nodes                        |  MySQL tables          | Dynamodb        |
|  __Scalar Propagation__    |   __sp__        | __Propagate child scalar data into parent node__            |  __MySQL tables__          | __Dynamodb__        |
|  Double Propagation    |   dp        | Propagate grandchild scalar data into grandparent node* |  MySQL tables          | Dynamodb        |
|  ElasticSearch         |   es        | Load data into ElasticSearch                 |  MySQL tables          | Dynamodb        |


* for 1:1 relationships between grandparent and grandchild nodes

## GoGraph Design Guide 

