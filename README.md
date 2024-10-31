## What is SP?

Scalar Propagation (SP) is one of the data load components of GoGraph, a rudimentary graph database, developed originally in Go and now refactored in Rust. GoGraph is a high concurrent design making use of the Tokio asynchronous programming model. It is designed to support internet scale data volumes and user populations. It currently supports AWS's Dynamodb, although other hyper scalable database solutions such as Spanner are also envisioned. 

## GoGraph Load Components

| Load Compoent          | Abreviation |  Task                                       |  Data Source           |
|-----------------------:|-------------|---------------------------------------------|------------------------|
|  RDF-Loader            |   Ldr       | Load RDF file into Dynamodb and MySQL       |  RDF file              |
|  Attach               2|   At        | Link child nodes to parent nodes            |  MySQL tables          |
|  Scalar Propagation   3|   SP        | Propagate scalar data into parent node      |  MySQL tables          | 
|  Double Propagation   4|   DP        | Progation scalar data into grandparent node |  MySQL tables          |
|  ElasticSearch        5|   Es        | Load data into ElasticSearch                |  MySQL tables          |
