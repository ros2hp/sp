## What is SP?

Scalar Propagation (SP) is one of the data load components of GoGraph, a rudimentary graph database, developed originally in Go and now refactored in Rust. GoGraph is a high concurrent design making use of the Tokio asynchronous programming model. It is designed to support internet scale data volumes and user populations. It currently supports AWS's Dynamodb, although other hyper scalable database solutions such as Spanner are also envisioned. 

## GoGraph Load Components

SP is the third in the sequence of five components that comprise the GoGraph RDF load process.

| Load Compoent          | Abreviation |  Task                                                   |  Data Source           | Target Database |
|-----------------------:|-------------|---------------------------------------------------------|------------------------|-----------------|
|  RDF-Loader            |   Ldr       | Load RDF file into Dynamodb and MySQL                   |  RDF file              | Dynamodb, MySQL |
|  Attach                |   At        | Link child nodes to parent nodes in Dynamodb            |  MySQL tables          | Dynamodb        |
|  Scalar Propagation    |   SP        | Propagate child scalar data into parent node            |  MySQL tables          | Dynamodb        |
|  Double Propagation    |   DP        | Progation grandchild scalar data into grandparent node  |  MySQL tables          | Dynamodb        |
|  ElasticSearch         |   Es        | Load data into ElasticSearch                 |  MySQL tables          | Dynamodb        |
