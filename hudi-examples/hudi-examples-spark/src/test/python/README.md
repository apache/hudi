<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
-->
# Requirements
Python is required to run this. Pyspark 2.4.7 does not work with the latest versions of python (python 3.8+) so if you want to use a later version (in the example below 3.3) you can build Hudi by using the command:
```bash
cd $HUDI_DIR
mvn clean install -DskipTests -Dspark3.3 -Dscala2.12 
```
Various python packages may also need to be installed so you should get pip and then use **pip install \<package name\>** to get them
# How to Run
1. [Download pyspark](https://spark.apache.org/downloads)
2. Extract it where you want it to be installed and note that location
3. Run(or add to .bashrc) the following and make sure that you put in the correct path for SPARK_HOME
```bash
export SPARK_HOME=/path/to/spark/home
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_SUBMIT_ARGS="--master local[*]"
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/*.zip:$PYTHONPATH
```
4. Identify the Hudi Spark Bundle .jar or package that you wish to use:
A package will be in the format **org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0**
A jar will be in the format  **\[HUDI_BASE_PATH\]/packaging/hudi-spark-bundle/target/hudi-spark-bundle\[VERSION\].jar**
5. Go to the hudi directory and run the quickstart examples using the commands below, using the -t flag for the table name and the -p flag or -j flag for your package or jar respectively.
```bash
cd $HUDI_DIR
python3 hudi-examples/hudi-examples-spark/src/test/python/HoodiePySparkQuickstart.py [-h] -t TABLE (-p PACKAGE | -j JAR)
```