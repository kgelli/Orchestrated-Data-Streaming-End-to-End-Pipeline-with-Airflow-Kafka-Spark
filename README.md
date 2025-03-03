# Realtime Streaming Data Pipeline

## Project Summary
This real-time streaming data pipeline project offers a comprehensive end-to-end data engineering solution that handles the complete data lifecycle. The project integrates multiple big data technologies to create a robust, scalable system for processing streaming data in real time.

The pipeline begins with data ingestion through an API, which feeds into an orchestration layer powered by Apache Airflow. From there, data flows into Apache Kafka, a distributed event streaming platform that acts as the central nervous system of the architecture. Apache ZooKeeper provides coordination services for Kafka.

Once in Kafka, the data is processed by a distributed Apache Spark cluster consisting of a master node and multiple worker nodes. After processing, the data is stored in Cassandra, a distributed NoSQL database designed to handle large amounts of data across multiple servers. The entire system is containerized using Docker, making it easy to deploy and scale.

## Architecture
![System Architecture](architecture_diagram.png)

The architecture follows a logical flow:

1. **Ingestion Layer**: An API serves as the entry point for data, which is then managed by Apache Airflow for workflow orchestration. Airflow uses PostgreSQL as its metadata database.

2. **Messaging Layer**: Apache Kafka, supported by ZooKeeper, handles the real-time streaming of data. Kafka's Control Center and Schema Registry provide monitoring and schema management capabilities.

3. **Processing Layer**: A distributed Apache Spark cluster processes the streaming data. The cluster consists of a Spark master and multiple Spark workers, enabling parallel processing of large data volumes.

4. **Storage Layer**: Processed data is ultimately stored in Cassandra, which provides high availability and fault tolerance for the data.

This architecture ensures high throughput, fault tolerance, and scalability, making it suitable for applications requiring real-time data processing such as log analysis, IoT data processing, real-time analytics, and monitoring systems.

## Tools Used

* **Apache Airflow**: Workflow orchestration platform that programmatically schedules and monitors data pipelines.
* **PostgreSQL**: Object-relational database system that stores Airflow metadata and configuration.
* **Apache Kafka**: Distributed event streaming platform for high-throughput, fault-tolerant real-time data feeds.
* **Apache ZooKeeper**: Centralized service for maintaining configuration information and distributed synchronization for Kafka.
* **Apache Spark**: Unified analytics engine for large-scale distributed data processing with in-memory computation capabilities.
* **Cassandra**: Highly scalable, high-performance distributed NoSQL database designed to handle large amounts of data across commodity servers.
* **Kafka Control Center**: Web-based tool for managing and monitoring Kafka clusters, topics, and data streams.
* **Schema Registry**: Service that provides a RESTful interface for storing and retrieving Kafka message schemas.
* **Docker**: Containerization platform that packages applications and their dependencies together for consistent deployment across environments.
* **Python**: Programming language used for developing data processing logic, Airflow DAGs, and Spark applications.

## Conclusion
This dockerized real-time streaming data pipeline represents a modern approach to data engineering that addresses the challenges of processing high-volume, high-velocity data. By leveraging containerization, the solution offers portability across environments and simplifies deployment.

The combination of Airflow, Kafka, Spark, and Cassandra creates a powerful ecosystem capable of handling enterprise-grade data workloads while maintaining the flexibility to scale horizontally as data volumes grow. This architecture is particularly valuable for organizations seeking to implement real-time analytics, event-driven architectures, or any application requiring immediate insights from streaming data.
