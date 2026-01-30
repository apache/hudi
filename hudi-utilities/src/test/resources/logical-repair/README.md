
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

Test assets

trips_logical_types_json_cow_write.zip:

this table was created with two deltastreamer writes:

write 0 with 0.15.0:
inserts to partition 1, partition 2, partition 3

write 1 with 0.15.0:
inserts to partition 3

this gives us a table with 3 partitions, partition 1 and 2 have 1 file each and partition 3 has 2.

Then we provide updates in cow_write_updates:

write 2 done in the test:
inserts to partition 3, partition 4

write 3 done in the test:
updates to partition 3

this gives us a final table:

partition 1:
1 base file written with 0.15.0
partition 2:
1 base file written with 0.15.0
1 base file written with 1.1
partition 3:
1 base file written with 1.1 that contains some 0.15.0 written records
1 base file written with 0.15.0
1 base file written with 1.1
partition 4:
1 base file written with 1.1


trips_logical_types_json_mor_write_avro_log.zip/trips_logical_types_json_mor_write_parquet_log.zip
the two tables were created with the same steps, but the avro table uses avro log files and the parquet table uses parquet files

write 0 with 0.15.0:
inserts to partition 1, 2, 3

write 1 with 0.15.0:
inserts to partition 3

write 2 with 0.15.0:
updates to 1 file in partition 3 and 1 file in partition 2

write 3 with 0.15.0:
inserts to partition 3

write 4 with 0.15.0
inserts to partition 3 and updates to 1 file in partition 3

write 5 done in the tests:
updates to 2 files in partition 3 and inserts to partition 3

The final table will be

partition 1:
fg1: base file with 0.15.0
partition 2:
fg1: base file with 0.15.0 + log file with 0.15.0
partition 3:
fg1: base file with 0.15.0 + log file with 0.15.0 + log file with 1.1
fg2: base file with 0.15.0 + log file with 1.1
fg3: base file with 1.1
fg4: base file with 0.15 + log file with 0.15
fg5: base file with 0.15
