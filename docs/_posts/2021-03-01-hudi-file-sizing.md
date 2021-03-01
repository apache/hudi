---
title: "Apache Hudi File Sizing"
excerpt: "How Apache hudi manages to maintain optimum sized files to maintain read SLAs"
author: shivnarayan
category: blog
---

Apache Hudi is a data platform technology that provides several functionalities needed to build and manage a data lake. 
One of the key features that hudi provides for you is self managing file sizing so that users don’t need to worry about 
small files in their dataset. Having a lot of small files will make it harder to maintain your SLAs for read queries. 
But for streaming data lake use-cases, inherently ingests are going to end up having smaller volume of writes, which 
might result in lot of small files if no special management is done.

# Apache Hudi file size management

Hudi avoids such small files and always writes properly sized files, taking a slight hit on ingestion but guaranteeing 
SLAs for your read queries. Common approaches to writing very small files and then later stitching them together only 
solve for system scalability issues posed by small files and also let queries slow down by exposing small files to 
them anyway. You could leverage clustering feature that’s part of Hudi, but since you can’t run it frequently, self 
managed file sizing will be critical for anyone who is looking to manage their data lakes.

Hudi has the ability to maintain a configured target file size, when performing inserts/upsert operations. 
(Note: bulk_insert operation does not provide this functionality and is designed as a simpler replacement for 
normal `spark.write.parquet`).

This blog illustrates how hudi does small file management or self manages the file sizing for you. For illustration 
purposes, we are going to consider only COPY_ON_WRITE table.

## Configs

Configs of interest before we dive into the algorithm:

[Max file size](https://hudi.apache.org/docs/configurations.html#limitFileSize): Max size for a given data file. Hudi 
will try to maintain file sizes to this configured value <br/>
[Soft file limit](https://hudi.apache.org/docs/configurations.html#compactionSmallFileSize): Max file size below which 
a given data file is considered to a small file <br/>
[Insert split size](https://hudi.apache.org/docs/configurations.html#insertSplitSize): Number of inserts grouped for a single partition. This value should match the number of records 
in a single file (you can determine based on max file size and per record size)

For instance, if your first config value is 120Mb and 2nd config value is set to 100Mb, any file whose size is < 100Mb 
are considered to be a small file.

If you wish to turn off this feature (small file management feature, which hudi does not recommend in general), 
set the config value for soft file limit to 0.

## File size management in Hudi

The algorithm is generally applicable to any partitions and so our illustration is just going to focus on a single 
partition only. Let’s say this is the layout of data files for a given partition.

![Initial layout](/assets/images/blog/hudi-file-sizing/initial_layout.png)
_Figure: Initial data file sizes for a given partition of interest_

Let’s assume the configured values for max file size and small file size limit is 120Mb and 100Mb. File_1’s current 
size is 40Mb, File_2’s size is 80Mb, File_3’s size is 90Mb, File_4’s size is 130Mb and File_5’s size is 105Mb.

Let’s see what happens when a new write batch is ingested to hudi. This is done in multiple steps. First, updates are 
assigned to respective files followed which inserts are assgined with the data files.

Step1: Assigning updates to files. In this step, index is looked up to find the tagged location and records are 
assigned to respective files. Note: Updates are not going to alter the file size as unique records per file will remain
the same.

Step2:  Determine small files for the partition of interest. The soft file limit config value will be leveraged here 
to determine what constitutes as a small file. Given the config value is set to 100Mb, the small files are File_1(40Mb)
and File_2(80Mb) and file_3’s (90Mb). No new inserts will go into any of the other existing files as they don’t qualify 
for small files.

Stpe3: Once small files are determined, incoming inserts are assigned to them so that they reach their max capacity of 
120Mb. File_1 will be ingested with 80Mb worth of inserts, file_2 will be ingested with 40Mb worth of inserts and 
File_3 will be ingested with 30Mb worth of inserts.

![Bin packing small files](/assets/images/blog/hudi-file-sizing/bin_packing_existing_data_files.png)
_Figure: Incoming records are bin packed to existing small files_

Step4: Once all small files are bin packed to its max capacity and if there are pending inserts unassigned, new file 
Ids are created and inserts are assigned to them. Number of Records per new data file is determined from insert split 
size config. Assuming the insert split size is configured to 120k records, if there are 300k remaining records, 3 new 
files will be created in which 2 of them (File_6 and File_7) will be filled with 120k records and the last one (File_8)
will be filled with 60k records (assuming each record is 1000 bytes). In future ingestions, 3rd new file will be 
considered as a small file to be packed with more data.

![Assigning to new files](/assets/images/blog/hudi-file-sizing/adding_new_files.png)
_Figure: Remaining records are assigned to new files_

Hudi has a custom partitioner which will be leveraged by the execution engine and records will be distributed according
to the assignment done as per the algorithm shown above.

After this ingestion is complete, except File_8, every other file is nicely sized to its optimum size. This process is
followed during every ingestion to ensure there are no small files in your Hudi data lake or atleast future ingestions 
will try to bin pack records to small files to ensure optimum file sizes. Thus, your read SLAs will stay within bounds
as there are no small files with Hudi.

It’s an operational nightmare to manage this manually or stitch at more frequent intervals and hence hudi provides such
auto file sizing capability for your data lakes. Hopefully the blog gave you a good sneak peek into how hudi manages 
small files and assists in boosting your read latencies.  
