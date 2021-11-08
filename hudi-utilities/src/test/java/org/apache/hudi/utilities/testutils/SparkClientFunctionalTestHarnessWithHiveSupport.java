package org.apache.hudi.utilities.testutils;

import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.spark.SparkConf;

import java.util.Collections;

public class SparkClientFunctionalTestHarnessWithHiveSupport extends SparkClientFunctionalTestHarness {

  public SparkConf conf() {
    return conf(Collections.singletonMap("spark.sql.catalogImplementation", "hive"));
  }
}
