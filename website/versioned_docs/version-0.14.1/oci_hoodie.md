---
title: Oracle Cloud Infrastructure
keywords: [ hudi, hive, oracle cloud, storage, spark ]
summary: In this page, we go over how to configure hudi with Oracle Cloud Infrastructure Object Storage.
last_modified_at: 2022-03-03T16:57:05-08:00
---
The [Oracle Object Storage](https://docs.oracle.com/en-us/iaas/Content/Object/Concepts/objectstorageoverview.htm) system provides strongly-consistent operations on all buckets in all regions. OCI Object Storage provides an [HDFS Connector](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/hdfsconnector.htm) your Application will need to access data.

## OCI Configs

To use HUDI on OCI Object Storage you must:

- Configure the HDFS Connector using an API key
- Include the HDFS Connector and dependencies in your application
- Construct an OCI HDFS URI

### Configuring the HDFS Connector

The OCI HDFS Connector requires configurations from an API key to authenticate and select the correct region. Start by [generating an API key](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm).

If you are using Hadoop, include these in your core-site.xml:

```xml
  <property>
    <name>fs.oci.client.auth.tenantId</name>
    <value>ocid1.tenancy.oc1..[tenant]</value>
    <description>The OCID of your OCI tenancy</description>
  </property>

  <property>
    <name>fs.oci.client.auth.userId</name>
    <value>ocid1.user.oc1..[user]</value>
    <description>The OCID of your OCI user</description>
  </property>

  <property>
    <name>fs.oci.client.auth.fingerprint</name>
    <value>XX::XX</value>
    <description>Your 32-digit hexidecimal public key fingerprint</description>
  </property>

  <property>
    <name>fs.oci.client.auth.pemfilepath</name>
    <value>/path/to/file</value>
    <description>Local path to your private key file</description>
  </property>

  <property>
    <name>fs.oci.client.auth.hostname</name>
    <value>https://objectstorage.[region].oraclecloud.com</value>
    <description>HTTPS endpoint of your regional object store</description>
  </property>
```

If you are using Spark outside of Hadoop, set these configurations in your Spark Session:

| Key                                         | Description                                      |
| ------------------------------------------- | ------------------------------------------------ |
| spark.hadoop.fs.oci.client.auth.tenantId    | The OCID of your OCI tenancy                     |
| spark.hadoop.fs.oci.client.auth.userId      | The OCID of your OCI user                        |
| spark.hadoop.fs.oci.client.auth.fingerprint | Your 32-digit hexidecimal public key fingerprint |
| spark.hadoop.fs.oci.client.auth.pemfilepath | Local path to your private key file              |
| spark.hadoop.fs.oci.client.hostname         | HTTPS endpoint of your regional object store     |

If you are running Spark in OCI Data Flow you do not need to configure these settings, object storage access is configured for you.

### Libraries

These libraries need to be added to your application. The versions below are a reference, the libraries are continuously updated and you should check for later releases in Maven Central.

- com.oracle.oci.sdk:oci-java-sdk-core:2.18.0
- com.oracle.oci.sdk:oci-hdfs-connector:3.3.0.5

### Construct an OCI HDFS URI

OCI HDFS URIs have the form of:

`oci://<bucket>@<namespace>/<path>`

The HDFS connector allows you to treat these locations similar to an `HDFS` location on Hadoop. Your tenancy has a unique Object Storage namespace. If you're not sure what your namespace is you can find it by installing the [OCI CLI](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm) and running `oci os ns get`.