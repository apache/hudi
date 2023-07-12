/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hudi.common.config.DFSPropertiesConfiguration
import org.apache.hudi.common.config.TypedProperties;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.PrintStream

// init
val conf = spark.sparkContext.hadoopConfiguration
val dfsBasePath = "/tmp/hudi"
val dfsPath = new Path(dfsBasePath)

val dfs = dfsPath.getFileSystem(conf)

def writePropertiesFile(path: Path, lines: String*) {
  val out: PrintStream = new PrintStream(dfs.create(path), true)
  for (line <- lines) {
    out.println(line);
  }
  out.flush();
  out.close();
}

def cleanUpGlobalConfig() {
  DFSPropertiesConfiguration.clearGlobalProps
}

// Tests
// testParsing
def testParsing() {
  val cfg = new DFSPropertiesConfiguration(dfs.getConf(), new Path(dfsBasePath + "/t1.props"));
  val props = cfg.getProps

  assert(props.size == 5)
  try {
    props.getString("invalid.key")
    assert(false, "Should error out here")
  } catch {
    case iae: IllegalArgumentException => "Pass"
  }

  assert(props.getInteger("int.prop") == 123)
  assert(props.getDouble("double.prop") == 113.4)
  assert(props.getBoolean("boolean.prop"))
  assert(props.getString("string.prop") == "str")
  assert(props.getLong("long.prop") == 1354354354)

  assert(props.getInteger("int.prop", 456) == 123)
  assert(props.getDouble("double.prop", 223.4) == 113.4)
  assert(props.getBoolean("boolean.prop", false))
  assert(props.getString("string.prop", "default") == "str")
  assert(props.getLong("long.prop", 8578494434L) == 1354354354)

  assert(props.getInteger("bad.int.prop", 456) == 456)
  assert(props.getDouble("bad.double.prop", 223.4) == 223.4)
  assert(!props.getBoolean("bad.boolean.prop", false))
  assert(props.getString("bad.string.prop", "default") == "default")
  assert(props.getLong("bad.long.prop", 8578494434L) == 8578494434L)

  cleanUpGlobalConfig()
}

def testIncludes() {
  val cfg = new DFSPropertiesConfiguration(dfs.getConf(), new Path(dfsBasePath + "/t3.props"));
  val props = cfg.getProps

  assert(props.getInteger("int.prop") == 123)
  assert(props.getDouble("double.prop") == 243.4)
  assert(props.getBoolean("boolean.prop"))
  assert(props.getString("string.prop") == "t3.value")
  assert(props.getLong("long.prop") == 1354354354)

  try {
    cfg.addPropsFromFile((new Path(dfsBasePath + "/t4.props")))
    assert(false, "Should error out here")
  } catch {
    case ise: IllegalStateException => "Pass"
  }

  cleanUpGlobalConfig()
}

// create some files
val filePath1 = new Path(dfsBasePath + "/t1.props")
writePropertiesFile(filePath1, "", "#comment", "abc",
  "int.prop=123", "double.prop=113.4", "string.prop=str", "boolean.prop=true", "long.prop=1354354354")

val filePath2 = new Path(dfsBasePath + "/t2.props")
writePropertiesFile(filePath2, "string.prop=ignored", "include=" + dfsBasePath + "/t1.props")

val filePath3 = new Path(dfsBasePath + "/t3.props")
writePropertiesFile(filePath3, "double.prop=838.3", "include=" + "t2.props", "double.prop=243.4", "string.prop=t3.value")

val filePath4 = new Path(dfsBasePath + "/t4.props")
writePropertiesFile(filePath4, "double.prop=838.3", "include = t4.props");

try {
  // Run tests
  testParsing()
  testIncludes()
} catch {
  case ae: AssertionError => System.exit(1)
}


System.exit(0)