---
title: Code Structure
keywords: usecases
sidebar: mydoc_sidebar
permalink: code_and_design.html
---

## Code & Project Structure

 * hoodie-client     : Spark client library to take a bunch of inserts + updates and apply them to a Hoodie table
 * hoodie-common     : Common code shared between different artifacts of Hoodie

 ## HoodieLogFormat

 The following diagram depicts the LogFormat for Hoodie MergeOnRead. Each logfile consists of one or more log blocks.
 Each logblock follows the format shown below.

 | Field  | Description |
 |-------------- |------------------|
 | MAGIC    | A magic header that marks the start of a block |
 | VERSION  | The version of the LogFormat, this helps define how to switch between different log format as it evolves |
 | TYPE     | The type of the log block |
 | HEADER LENGTH | The length of the headers, 0 if no headers |
 | HEADER        | Metadata needed for a log block. For eg. INSTANT_TIME, TARGET_INSTANT_TIME, SCHEMA etc. |
 | CONTENT LENGTH |  The length of the content of the log block |
 | CONTENT        | The content of the log block, for example, for a DATA_BLOCK, the content is (number of records + actual records) in byte [] |
 | FOOTER LENGTH  | The length of the footers, 0 if no footers |
 | FOOTER         | Metadata needed for a log block. For eg. index entries, a bloom filter for records in a DATA_BLOCK etc. |
 | LOGBLOCK LENGTH | The total number of bytes written for a log block, typically the SUM(everything_above). This is a LONG. This acts as a reverse pointer to be able to traverse the log in reverse.|


 {% include image.html file="hoodie_log_format_v2.png" alt="hoodie_log_format_v2.png" %}






