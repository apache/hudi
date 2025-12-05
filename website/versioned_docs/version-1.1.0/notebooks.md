---
title: "Notebooks"
keywords: [ hudi, notebooks ]
toc: true
last_modified_at: 2025-10-09T19:13:57+08:00
---

Get hands-on with Apache Hudi using interactive notebooks!

This demo environment is designed to help you explore Hudi's capabilities end-to-end using real-world scenarios. It runs entirely within a local Docker-based infrastructure, making it easy to get started without any external dependencies. The setup includes all required services, such as Spark, Hive, and a local S3-compatible store, bundled and orchestrated via Docker Compose.

All you need is a cloned copy of the Hudi repository and Docker installed on your system. These notebooks have been tested on macOS.

### Setup

* Clone the [Hudi repository](https://github.com/apache/hudi) to your local machine.
* Docker Setup: For macOS, follow the steps in [Install Docker Desktop on Mac](https://docs.docker.com/desktop/install/mac-install/). For Spark SQL queries, ensure at least 6 GB of memory and 4 CPUs are allocated to Docker (see Docker > Preferences > Advanced).
* Build Docker Images

  ```shell
  # under Hudi repo root dir
  cd hudi-notebooks
  sh build.sh
  ```

* Start the Environment

  ```shell
  sh run_spark_hudi.sh start
  ```
  
You can explore and practice our example notebooks directly from your browser! Once your environment is up and running, open http://localhost:8888 to access Jupyter Notebook. From there, you can explore, edit, and run the provided notebooks, and build hands-on experience in a fully interactive workspace.

### Meet Your Notebooks

#### 1 - Getting Started with Apache Hudi: A Hands-On Guide to CRUD Operations

This notebook is a beginner-friendly, practical guide to working with Apache Hudi using PySpark. It walks you through the essential CRUD operations (Create, Read, Update, Delete) on Hudi tables, while also helping you understand key table types such as Copy-On-Write (COW) and Merge-On-Read (MOR).

For storage, we use MinIO as an S3-compatible backend.

**What you will learn:**

* How to create and update Hudi tables using PySpark
* The difference between COW and MOR tables
* Reading data using snapshot and incremental queries
* How Hudi handles upserts and deletes

#### 2 - Deep Dive into Apache Hudi Table & Query Types: Snapshot, RO, Incremental, Time Travel, CDC

This notebook is your hands-on guide to mastering Apache Hudi's advanced query capabilities. You will explore practical examples of various read modes such as Snapshot, Read-Optimized (RO), Incremental, Time Travel, and Change Data Capture (CDC) so you can understand when and how to use each for building efficient, real-world data pipelines.

**What you will learn:**

* How to perform Snapshot and Read-Optimized queries
* Using incremental pulls for near-real-time data processing
* Querying historical data with Time Travel
* Capturing changes with CDC for downstream consumption

#### 3 - Implementing Slowly Changing Dimensions (SCD Type 2 & 4) with Apache Hudi

Dive into this practical guide on implementing two key data warehousing patterns - Slowly Changing Dimensions (SCD) Type 2 and Type 4 using Apache Hudi.

SCDs help track changes in dimension data over time without losing historical context. Instead of overwriting records, these patterns let you maintain a full history of data changes. Leveraging Hudi's upsert capabilities and rich metadata, this notebook simplifies what is traditionally a complex process.

**What you will learn:**

* SCD Type 2: How to track changes by adding new rows to your dimension tables
* SCD Type 4: How to manage historical data in a separate history table

#### 4 - Schema Evolution with Apache Hudi: Concepts and Practical Use

In real-world data lakehouse environments, schema changes are not just common—they are expected. Whether you are adding new data attributes, adjusting existing types, or refactoring nested structures, it is essential that your pipelines adapt without introducing instability.

Apache Hudi supports powerful schema evolution capabilities that help you maintain schema flexibility while ensuring data consistency. In this notebook, we will explore how Hudi enables safe and efficient schema changes, both at write time and read time.

**What you will learn:**

* Schema Evolution on Write:
Apache Hudi allows safe, backward-compatible schema changes during write operations. This ensures that you can evolve your schema without rewriting existing data or breaking your ingestion pipelines.
* Schema Evolution on Read:
Hudi also supports schema evolution during reads, enabling more flexible transformations that do not require rewriting the dataset.

#### 5 - A Hands-On Guide to Hudi SQL Procedures

Apache Hudi provides a suite of powerful built-in procedures that can be executed directly from Spark SQL using the familiar CALL syntax.

These procedures enable you to perform advanced table maintenance, auditing, and data management tasks without writing any custom code or scripts. Whether you are compacting data, cleaning old versions, or retrieving metadata, Hudi SQL procedures make it easy and SQL-friendly.

**What you will learn:**

In this guide, we will explore how to use various Hudi SQL procedures through practical, real-world examples. You will learn how to invoke these operations using Spark SQL and understand when and why to use each one.
