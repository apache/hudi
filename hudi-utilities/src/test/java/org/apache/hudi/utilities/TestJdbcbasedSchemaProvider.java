package org.apache.hudi.utilities;

import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.JdbcbasedSchemaProvider;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class TestJdbcbasedSchemaProvider {

  private static final Logger LOG = LogManager.getLogger(TestJdbcbasedSchemaProvider.class);
  private static final TypedProperties PROPS = new TypedProperties();
  protected transient JavaSparkContext jsc = null;

  @Before
  public void init() {
    jsc = UtilHelpers.buildSparkContext(this.getClass().getName() + "-hoodie", "local[2]");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.jdbc.connection.url", "jdbc:h2:mem:test_mem");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.jdbc.driver.type", "org.h2.Driver");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.jdbc.username", "sa");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.jdbc.password", "");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.jdbc.dbtable", "triprec");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.jdbc.timeout", "0");
    PROPS.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.jdbc.nullable", "false");
  }

  @After
  public void teardown() throws Exception {
    if (jsc != null) {
      jsc.stop();
    }
  }

  @Test
  public void testJdbcbasedSchemaProvider() throws Exception {
    try {
      initH2Database();
      Schema sourceSchema = UtilHelpers.createSchemaProvider(JdbcbasedSchemaProvider.class.getName(), PROPS, jsc).getSourceSchema();
      assertEquals(sourceSchema.toString().toUpperCase(), new Schema.Parser().parse(UtilitiesTestBase.Helpers.readFile("delta-streamer-config/source-jdbc.avsc")).toString().toUpperCase());
    } catch (HoodieException e) {
      LOG.error("Failed to get connection through jdbc. ", e);
    }
  }

  private void initH2Database() throws SQLException, IOException {
    Connection conn = DriverManager.getConnection("jdbc:h2:mem:test_mem", "sa", "");
    PreparedStatement ps = conn.prepareStatement(UtilitiesTestBase.Helpers.readFile("delta-streamer-config/triprec.sql"));
    ps.executeUpdate();
  }
}
