
# RFC-92: Pluggable Table Formats in Hudi

## Proposers

*   Balaji Varadarajan

## Approvers

*   Vinoth Chandar
*   Ethan Guo

## Status

JIRA: <TBD>

## Abstract

This RFC proposes support for different backing table format implementations inside Hudi. For the past 4 years at-least, we have been consistently defining Hudi as a broader platform and software [stack](https://hudi.apache.org/docs/hudi_stack) that delivers much of these benefits. Hudi's table format makes choices specific to data lake workloads, allowing efficient read/write (even the recent [blog](https://bytearray.substack.com/p/computer-science-behind-lakehouse) from Vinoth), has major differences and advantages compared to other approaches. The community plans to centrally focus on the native Hudi storage format.

However, there may be benefits to allowing other storage layouts/table formats to fit under Hudi's higher level functionality. This also has non-technical benefits of insulating the project from vendor marketing wars. Most contributors (such as myself) are happily part of the global Hudi open-source community, for the sake of just building technology.

## Background

Expanding further, there are plenty of valid technical reasons on why Hudi should allow different storage layouts, under the upper layer reader/writer and table services implementations.

1. We have use-cases, for cloud-native/high performance implementations of timeline (\`HoodieTimeline\`) and metadata (\`HoodieMetadata\` interface). In our use-case, we would like to explore backing them using NoSQL datastore like DynamoDB, for ultra-low latency queries.
2. Hudi already supports [different](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/storage/HoodieStorageLayout.java) storage formats/layouts. Tables can be bucketed, consistent hashed or organized by having data laid out in order of arrival (default).
3. Hudi already allows plug-ability and customization at various layers like record merger, indexes and other core write/read paths.
4. It's very standard practice in databases to allow multiple storage backends (MySQL supports myISAM, innodb/btree, myrocks/lsm). This may be crucial step towards the database northstar vision.
5. As a long time member of the Hudi community and open-source enthusiast, I think supporting other table formats, even existing ones like Apache Iceberg or Delta Lake, benefits those communities as well.
    1. For e.g. the Hudi Streamer tool being used at our data lake (and hundreds more) for ingestion/incremental ETL can also benefit other communities.
    2. Hudi provides out-of-box automatic table management that is manual in projects like Iceberg. With such an implementation, common data lake services can be reused across formats.
    3. Hudi's high performance writer path can be extended to other formats (to the extent possible, that is not dependent on features like indexing that is only in Hudi's native format)
    4. there are more such services and functionalities to be unlocked.


Some non-technical reasons:
1. Though Hudi is clearly defined as a platform over the years, there is so much vendor attention in the space for the past couple years, where Hudi is minimized to a table format and compared. This change will help highlight the value of Hudi's open software services, beyond just open formats.
2. It may be controversial to say this. But, the project has been facing a lot of vendor FUD due to different vendors supporting different table formats. It is neither in the interest nor the business of the project community to be part of vendor wars. Opening up the table format layer to different implementations avoids these distractions for regular OSS contributors with no vendor interests, and helps focus on open-source software design and development.

  
## **Implementation**

The main implementation step here is to create abstraction called TableFormatPlugin which handles table format operations such as 


1. Committing writes 
2. Update and read table format specific Metadata operations 
3. Timeline 
4. Conflict Resolution
5. Lock Provider
6. Rollbacks


The Hudi platform is responsible for managing the data path and can be configured with the table format plugin by default using Hudi native table format. Other table formats will have their own implementation of this abstraction.  

### Commit Protocol:

Hudi uses the creation of a .commit file to advertise the completion of a commit on the table. With the TableFormatPlugin, this will be a two step process:

1.  Creation of .commit file in .hoodie timeline 
2. Store the commit completion time in the table format's commit metadata.

  
With this, the write is only completed when both the above steps are completed. The plugin provided timeline needs to fence the timeline ensuring the definition of complete stays consistent.  This ensures the snapshot isolation is maintained.

### Metadata:

If a different table format is configured, Hudi's metadata operations are replaced with the table format's metadata. This is done by adding a new adapter implementation of HoodieTableMetadata.


### Timeline: 

The timeline implementation ensures the external table format's (table format that is plugged-in) metadata state (e:g commit status) is the source of truth for all operations. 

### Others:

The plugin implementation brings in the external table format's conflict resolution strategy and locking mechanisms. 

### Layout:

The metadata corresponding to the table format (iceberg, ..) will be stored under .hoodie/ folder.

### Codebase:

This implementation can co-exist in the Hudi codebase and will be shipped as separate modules (for eg -  hudi-dynamodb, hudi-iceberg, hudi-deltalake,...),  


The lakehouse platform interacts with this plugin and the table can be queried using the table format corresponding to the plugin. 

<TBD : Interface definition> 

## **Rollout/Adoption Plan**

*   For existing tables, utility to construct the table format for the first time.
*   Configuration of plugin to turn on specific formats. Default will be hudi.
*   Add support in 1.x

## **Test Plan**

TBD%
