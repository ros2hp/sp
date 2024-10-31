## What is SP?

Scalar Propagation (SP) is the third in the sequence of five components that comprise the GoGraph RDF load process. GoGraph, a rudimentary graph database, developed originally in Go and now refactored in Rust. GoGraph is a highly concurrent design making use of the Tokio asynchronous programming model. It is designed to support internet scale data volumes and user populations. It currently supports AWS's Dynamodb, although other hyper scalable database solutions, such as Google's Spanner, is also envisioned. 

## GoGraph Load Components

The table below lists the order of the program components that loads a RDF file into the Graph data model in Dynamodb. There is no size limit to the RDF file. MySQL is used as intermediary storage providing querying of the data as it is loaded into Dynamodb.  

| Load Compoent          | Abbreviation |  Task                                                   |  Data Source           | Target Database |
|-----------------------:|-------------|---------------------------------------------------------|------------------------|-----------------|
|  RDF-Loader            |   Ldr       | Load RDF file into Dynamodb and MySQL                   |  RDF file              | Dynamodb, MySQL |
|  Attacher              |   At        | Link child nodes to parent nodes in Dynamodb            |  MySQL tables          | Dynamodb        |
|  Scalar Propagation    |   SP        | Propagate child scalar data into parent node            |  MySQL tables          | Dynamodb        |
|  Double Propagation    |   DP        | Propagate grandchild scalar data into grandparent node* |  MySQL tables          | Dynamodb        |
|  ElasticSearch         |   Es        | Load data into ElasticSearch                 |  MySQL tables          | Dynamodb        |

* for 1:1 relationships between grandparent and grandchild nodes
