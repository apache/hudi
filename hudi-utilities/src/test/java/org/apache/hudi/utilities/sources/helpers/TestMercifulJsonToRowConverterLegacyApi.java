/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.sources.helpers;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

public class TestMercifulJsonToRowConverterLegacyApi extends TestMercifulJsonToRowConverterBase {

  private static final MercifulJsonToRowConverter CONVERTER = new MercifulJsonToRowConverter(true, "__", false);

  @BeforeAll
  public static void start() {
    spark = SparkSession
        .builder()
        .master("local[*]")
        .appName(TestMercifulJsonToRowConverterBase.class.getName())
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.datetime.java8API.enabled", "false")
        .getOrCreate();
  }

  @Override
  protected MercifulJsonToRowConverter getConverter() {
    return CONVERTER;
  }

  @Override
  protected boolean isJava8ApiEnabled() {
    return false;
  }
}
