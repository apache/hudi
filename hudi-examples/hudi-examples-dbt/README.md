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
## Testing dbt project: `hudi_examples_dbt`

This dbt project transforms demonstrates hudi integration with dbt, it has a few models to demonstrate the different ways in which you can create hudi datasets using dbt.

### What is this repo?
What this repo _is_:
- A self-contained playground dbt project, useful for testing out scripts, and communicating some of the core dbt concepts.

### Running this project
To get up and running with this project:
1. Install dbt using [these instructions](https://docs.getdbt.com/docs/installation).

2. Install [dbt-spark](https://github.com/dbt-labs/dbt-spark) package:
```bash
pip install dbt-spark
```

3. Clone this repo and change into the `hudi-examples-dbt` directory from the command line:
```bash
cd hudi-examples/hudi-examples-dbt
```

4. Set up a profile called `spark` to connect to a spark cluster by following [these instructions](https://docs.getdbt.com/reference/warehouse-profiles/spark-profile). If you have access to a data warehouse, you can use those credentials – we recommend setting your [target schema](https://docs.getdbt.com/docs/configure-your-profile#section-populating-your-profile) to be a new schema (dbt will create the schema for you, as long as you have the right privileges). If you don't have access to an existing data warehouse, you can also setup a local postgres database and connect to it in your profile.

> **NOTE:** You need to include the hudi spark bundle to the spark cluster, the latest supported version is 0.10.1.

5. Ensure your profile is setup correctly from the command line:
```bash
dbt debug
```

Output of the above command should show this text at the end of the output:
```bash
All checks passed!
```

6. Run the models:
```bash
dbt run
```

Output should look like this:
```bash
05:47:28  Running with dbt=1.0.0
05:47:28  Found 5 models, 10 tests, 0 snapshots, 0 analyses, 0 macros, 0 operations, 0 seed files, 0 sources, 0 exposures, 0 metrics
05:47:28
05:47:29  Concurrency: 1 threads (target='local')
05:47:29
05:47:29  1 of 5 START incremental model analytics.hudi_insert_table...................... [RUN]
05:47:31  1 of 5 OK created incremental model analytics.hudi_insert_table................. [OK in 2.61s]
05:47:31  2 of 5 START incremental model analytics.hudi_insert_overwrite_table............ [RUN]
05:47:34  2 of 5 OK created incremental model analytics.hudi_insert_overwrite_table....... [OK in 3.19s]
05:47:34  3 of 5 START incremental model analytics.hudi_upsert_table...................... [RUN]
05:47:37  3 of 5 OK created incremental model analytics.hudi_upsert_table................. [OK in 2.68s]
05:47:37  4 of 5 START incremental model analytics.hudi_upsert_partitioned_cow_table...... [RUN]
05:47:40  4 of 5 OK created incremental model analytics.hudi_upsert_partitioned_cow_table. [OK in 2.60s]
05:47:40  5 of 5 START incremental model analytics.hudi_upsert_partitioned_mor_table...... [RUN]
05:47:42  5 of 5 OK created incremental model analytics.hudi_upsert_partitioned_mor_table. [OK in 2.53s]
05:47:42
05:47:42  Finished running 5 incremental models in 14.70s.
05:47:42
05:47:42  Completed successfully
```
7. Test the output of the models:
```bash
dbt test
```
Output should look like this:
```bash
05:48:17  Running with dbt=1.0.0
05:48:17  Found 5 models, 10 tests, 0 snapshots, 0 analyses, 0 macros, 0 operations, 0 seed files, 0 sources, 0 exposures, 0 metrics
05:48:17
05:48:19  Concurrency: 1 threads (target='local')
05:48:19
05:48:19  1 of 10 START test not_null_hudi_insert_overwrite_table_id...................... [RUN]
05:48:19  1 of 10 PASS not_null_hudi_insert_overwrite_table_id............................ [PASS in 0.50s]
05:48:19  2 of 10 START test not_null_hudi_insert_overwrite_table_name.................... [RUN]
05:48:20  2 of 10 PASS not_null_hudi_insert_overwrite_table_name.......................... [PASS in 0.45s]
05:48:20  3 of 10 START test not_null_hudi_insert_overwrite_table_ts...................... [RUN]
05:48:20  3 of 10 PASS not_null_hudi_insert_overwrite_table_ts............................ [PASS in 0.47s]
05:48:20  4 of 10 START test not_null_hudi_insert_table_id................................ [RUN]
05:48:20  4 of 10 PASS not_null_hudi_insert_table_id...................................... [PASS in 0.44s]
05:48:20  5 of 10 START test not_null_hudi_upsert_table_id................................ [RUN]
05:48:21  5 of 10 PASS not_null_hudi_upsert_table_id...................................... [PASS in 0.38s]
05:48:21  6 of 10 START test not_null_hudi_upsert_table_name.............................. [RUN]
05:48:21  6 of 10 PASS not_null_hudi_upsert_table_name.................................... [PASS in 0.40s]
05:48:21  7 of 10 START test not_null_hudi_upsert_table_ts................................ [RUN]
05:48:22  7 of 10 PASS not_null_hudi_upsert_table_ts...................................... [PASS in 0.38s]
05:48:22  8 of 10 START test unique_hudi_insert_overwrite_table_id........................ [RUN]
05:48:23  8 of 10 PASS unique_hudi_insert_overwrite_table_id.............................. [PASS in 1.32s]
05:48:23  9 of 10 START test unique_hudi_insert_table_id.................................. [RUN]
05:48:24  9 of 10 PASS unique_hudi_insert_table_id........................................ [PASS in 1.26s]
05:48:24  10 of 10 START test unique_hudi_upsert_table_id................................. [RUN]
05:48:25  10 of 10 PASS unique_hudi_upsert_table_id....................................... [PASS in 1.29s]
05:48:26
05:48:26  Finished running 10 tests in 8.23s.
05:48:26
05:48:26  Completed successfully
05:48:26
05:48:26  Done. PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10
```

8. Generate documentation for the project:
```bash
dbt docs generate
```

9. View the [documentation](http://127.0.0.1:8080/#!/overview) for the project after running the following command:
```bash
dbt docs serve
```

---
For more information on dbt:
- Read the [introduction to dbt](https://docs.getdbt.com/docs/introduction).
- Read the [dbt viewpoint](https://docs.getdbt.com/docs/about/viewpoint).
- Join the [dbt community](http://community.getdbt.com/).
---
