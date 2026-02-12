# Presto Hudi Connector

##Overview

The **Presto Hudi Connector** enables querying Hudi tables synced to a Hive metastore. The connector uses the metastore only to track partition locations. It makes use of the underlying Hudi filesystem and input formats to list data files. To learn more about the design of the connector, please check out [RFC-40](https://github.com/apache/hudi/blob/master/rfc/rfc-44/rfc-44.md).

##Requirements

To use Hudi, we need:

* Network access from the Presto coordinator and workers to the distributed object storage.

* Access to a Hive metastore service (HMS).

* Network access from the Presto coordinator to the HMS. Hive metastore access with the Thrift protocol defaults to using port 9083.

##Configuration

Hudi supports the same metastore configuration properties as the Hive connector. At a minimum, following connector properties must be set in the `hudi.properties` file inside `<presto_install_dir> /etc/catalog` directory:

```
connector.name=hudi
hive.metastore.uri=thrift://hms.host:9083

```

Additionally, following session properties can be set depending on the use-case.

  Property Name   |        Description        | Default |
| ----------- | ----------- | ----------- |
 hudi.metadata-table-enabled   | Fetch the list of file names and sizes from Hudi’s metadata table rather than storage.  | false |
 
##SQL Support

Currently, the connector only provides read access to data in the Hudi table that has been synced to Hive metastore. Once the catalog has been configured as mentioned above, users can query the tables as usual like Hive tables.

##Supported Query Types

| Table Type   |        Supported Query types |  
| ----------- | ----------- |
| Copy On Write |     Snapshot Queries |    
| Merge On Read | Snapshot Queries + Read Optimized Queries | 



##Examples Queries

`trips_table` is a Hudi table that we refer to in the [Hudi quickstart documentation](https://hudi.apache.org/docs/quick-start-guide).

Here is a sample query:

``` 
USE hudi.default;
SELECT ts, fare, rider, driver, city FROM  trips_table WHERE fare > 20.0;
``` 

Output:

```
     ts       | fare  |  rider  |  driver  |        city        
---------------+-------+---------+----------+--------------------
 1695516137016 | 34.15 | rider-F | driver-P | city=sao_paulo     
 1695046462179 |  33.9 | rider-D | driver-L | city=san_francisco 
 1695091554788 |  27.7 | rider-C | driver-M | city=san_francisco 
```
 


#Historical
 
| **PrestoDB Version** | **Installation description** | **Query types supported** |
|----------------------|------------------------------|---------------------------|
| < 0.233              | Requires the `hudi-presto-bundle` jar to be placed into `<presto_install>/plugin/hive-hadoop2/`, across the installation. | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| > = 0.233             | No action needed. Hudi (0.5.1-incubating) is a compile time dependency. | Snapshot querying on COW tables. Read optimized querying on MOR tables. |
| > = 0.240             | No action needed. Hudi 0.5.3 version is a compile time dependency. | Snapshot querying on both COW and MOR tables. |
| > = 0.268             | No action needed. Hudi 0.9.0 version is a compile time dependency. | Snapshot querying on bootstrap tables. |
| > = 0.272             | No action needed. Hudi 0.10.1 version is a compile time dependency. | File listing optimizations. Improved query performance. |
| > = 0.275             | No action needed. Hudi 0.11.0 version is a compile time dependency. | All of the above. Native Hudi connector that is on par with Hive connector. |


> **Note**
>
>Incremental queries and point in time queries are not supported either through the Hive connector or Hudi
connector. However, it is in our roadmap, and you can track the development
under [HUDI-3210](https://issues.apache.org/jira/browse/HUDI-3210).
 
