/*
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
 * limitations under the License.
 */

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.JdbcbasedSchemaProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
public class TestJdbcbasedSchemaProvider extends SparkClientFunctionalTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestJdbcbasedSchemaProvider.class);
  private static final TypedProperties PROPS = new TypedProperties();

  @BeforeAll
  public static void init() {
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.jdbc.connection.url", "jdbc:h2:mem:test_mem");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.jdbc.driver.type", "org.h2.Driver");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.jdbc.username", "sa");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.jdbc.password", "");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.jdbc.dbtable", "triprec");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.jdbc.timeout", "0");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.jdbc.nullable", "false");
  }

  @Test
  public void testJdbcbasedSchemaProvider() throws Exception {
    try {
      initH2Database();
      Schema sourceSchema = Objects.requireNonNull(
        UtilHelpers.createSchemaProvider(JdbcbasedSchemaProvider.class.getName(), PROPS, jsc())).getSourceSchema();
      assertEquals(sourceSchema.toString().toUpperCase(), new Schema.Parser().parse(UtilitiesTestBase.Helpers.readFile("delta-streamer-config/source-jdbc.avsc")).toString().toUpperCase());
    } catch (HoodieException e) {
      LOG.error("Failed to get connection through jdbc. ", e);
    }
  }

  /**
   * Initialize the H2 database and obtain a connection, then create a table as a test.
   * Based on the characteristics of the H2 in-memory database, we do not need to display the initialized database.
   * @throws SQLException
   */
  private void initH2Database() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test_mem", "sa", "")) {
      PreparedStatement ps = conn.prepareStatement(UtilitiesTestBase.Helpers.readFile("delta-streamer-config/triprec.sql"));
      ps.executeUpdate();
    }
  }
}
