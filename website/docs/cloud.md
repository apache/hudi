---
title: Cloud Storage
keywords: [hudi, aws, gcp, oss, azure, cloud, juicefs]
summary: "In this page, we introduce how Hudi work with different Cloud providers."
toc: true
last_modified_at: 2021-10-12T10:50:00+08:00
---

## Talking to Cloud Storage

Immaterial of whether RDD/WriteClient APIs or Datasource is used, the following information helps configure access
to cloud stores.

* [AWS S3](/docs/s3_hoodie) <br/>
   Configurations required for S3 and Hudi co-operability.
* [Google Cloud Storage](/docs/gcs_hoodie) <br/>
   Configurations required for GCS and Hudi co-operability.
* [Alibaba Cloud OSS](/docs/oss_hoodie) <br/>
   Configurations required for OSS and Hudi co-operability.
* [Microsoft Azure](/docs/azure_hoodie) <br/>
   Configurations required for Azure and Hudi co-operability.
* [Tencent Cloud Object Storage](/docs/cos_hoodie) <br/>
   Configurations required for COS and Hudi co-operability.
* [IBM Cloud Object Storage](/docs/ibm_cos_hoodie) <br/>
   Configurations required for IBM Cloud Object Storage and Hudi co-operability.
* [Baidu Cloud Object Storage](bos_hoodie) <br/>
   Configurations required for BOS and Hudi co-operability.
* [JuiceFS](jfs_hoodie) <br/>
   Configurations required for JuiceFS and Hudi co-operability.
* [Oracle Cloud Infrastructure](oci_hoodie) <br/>
   Configurations required for OCI and Hudi co-operability.

:::note 
Many cloud object storage systems like [Amazon S3](https://docs.aws.amazon.com/s3/) allow you to set
[lifecycle policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html) to manage objects. 
One of the policies is related to object expiration. If your organisation has configured such policies, 
then please ensure to exclude (or have a longer expiry period) for Hudi tables.
:::