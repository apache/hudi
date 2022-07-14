<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
# RFC-56: DeltaStreamer Protobuf Support



## Proposers

- @the-other-tim-brown

## Approvers
- @bhasudha
- @vinothchandar

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-4399

> Please keep the status updated in `rfc/README.md`.

## Abstract

Support consuming Protobuf messages from Kafka with the DeltaStreamer.

## Background
Hudi's DeltaStreamer currently supports consuming Avro and JSON data from Kafka but it does not support Protobuf. Adding support will require:
1. Parsing the data from Kafka into Protobuf Messages
2. Generating a schema from a Protobuf Message class
3. Converting from Protobuf to Avro

## Implementation

### Parsing Data from Kafka
Users will provide a classname for the Protobuf Message that is contained within a jar that is on the path. We will then implement a deserializer that parses the bytes from the kafka message into a protobuf Message.

Configuration options:
hoodie.deltastreamer.schemaprovider.proto.className - The class to use

### ProtobufClassBasedSchemaProvider
This new SchemaProvider will allow the user to provide a Protobuf Message class and get an Avro Schema. In the proto world, there is no concept of a nullable field so people use wrapper types such as Int32Value and StringValue to represent a nullable field. The schema provider will also allow the user to treat these wrapper fields as nullable versions of the fields they are wrapping instead of treating them as a nested message. In practice, this means that the user can choose between representing a field `Int32Value my_int = 1;` as `my_int.value` or simply `my_int` when writing the data out to the file system.

Configuration Options:
hoodie.deltastreamer.schemaprovider.proto.className - The class to use
hoodie.deltastreamer.schemaprovider.proto.flattenWrappers (Default: false) - By default the wrapper classes will be treated like any other message and have a nested `value` field. When this is set to true, we do not have a nested `value` field and treat the field as nullable in the generated Schema

### ProtoToAvroConverter

A class will be created that can take in a Protobuf Message and convert it to an Avro GenericRecord. This will be used inside the SourceFormatAdapter to properly convert to an avro RDD. To convert to `Dataset<Row>` we will first convert to Avro and then to Row. This change will be adding a new `Source.SourceType` as well so other sources in the future can implement this source type, for example Protobuf messages on PubSub.

Special handling for maps:
Protobuf allows you to use any integral or string type as a key while Avro requires a string. To account for this, we convert all maps to lists of entries in that map.

## Rollout/Adoption Plan

This change simply adds new functionality and will not impact an existing users. The changes to the SourceFormatAdapter will only impact Proto source types and this is the first of its kind. Users will need to start running a new DeltaStreamer to get this functionality.

## Test Plan

- The new source will have testing that mirrors what we are currently doing for the TestJsonKafkaSource. This will exercise both reading the records from kafka into an `JavaRDD<GenericRecord>` and a `DataSet<Row>`
- The converter code will also be executed on the above path, but it will be more thoroughly tested in its own unit tests. The unit tests will use a sample protobuf message that covers nested messages, primitive, and wrapped field types.
- The schema provider will similarly be tested within its own unit tests to validate the behavior matches expectations.

