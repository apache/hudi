package org.apache.hudi.utilities.sources;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.utilities.UtilitiesTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJdbcSource extends UtilitiesTestBase {

  private final String hdfsSecretDir = "file:///tmp/hudi/config/";
  private final String secretFileName = "secret";
  private final String fullPathToSecret = hdfsSecretDir + secretFileName;
  private final String secretString = "hudi";
  private final String derbyDatabase = "sales";
  private final String derbyDir = String.format("/tmp/derby/%s", derbyDatabase);
  private final String derbyUrl = String.format("jdbc:derby:%s;create=true", derbyDir);
  Connection connection;
  PreparedStatement preparedStatement;
  final HoodieTestDataGenerator generator = new HoodieTestDataGenerator();
  TestJdbcSource testJdbcSource;

  private static Logger LOG = LoggerFactory.getLogger(JDBCSource.class);
  private TypedProperties props = new TypedProperties();

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
  }

  @Before
  @Override
  public void setup() throws Exception {
    super.setup();
    cleanDerby();
  }

  @After
  @Override
  public void teardown() throws Exception {
    super.teardown();
    cleanDerby();
    closeAllJdbcResources();
  }

  @Test
  public void testJDBCSourceWithSecret() {
    clear(props);
    prepareProps(props);
    addSecretStringToProps(props);
    Source jdbcSource = new JDBCSource(props, jsc, sparkSession, null);
    InputBatch inputBatch = jdbcSource.fetchNewData(Option.empty(), Long.MAX_VALUE);
    Dataset<Row> rowDataset = (Dataset<Row>) inputBatch.getBatch().get();
    LOG.info("Reading test output");
    rowDataset.show();
    long count = rowDataset.count();
    LOG.info("Num records in output {}", count);
    Assert.assertEquals(count, 4L);
  }

  private void addSecretFileToProps(TypedProperties props) {
    props.setProperty("hoodie.datasource.jdbc.password.file", fullPathToSecret);
  }

  private void addSecretStringToProps(TypedProperties props) {
    props.setProperty("hoodie.datasource.jdbc.password", secretString);
  }

  private void addSecretStringToProps(TypedProperties props, String secret) {
    props.setProperty("hoodie.datasource.jdbc.password", secret);
  }

  private void prepareProps(TypedProperties props) {
    props.setProperty("hoodie.datasource.jdbc.url", "jdbc:mysql://localhost:3306/");
    props.setProperty("hoodie.datasource.jdbc.user", "hudi");
    props.setProperty("hoodie.datasource.jdbc.table.name", "sales.contract");
    props.setProperty("hoodie.datasource.jdbc.driver.class", "com.mysql.cj.jdbc.Driver");
    props.setProperty("hoodie.datasource.jdbc.jar.path", "");
    props.setProperty("hoodie.datasource.write.recordkey.field", "contract_id");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "country");
    props.setProperty("hoodie.datasource.jdbc.incremental.pull", "true");
    props.setProperty("hoodie.datasource.jdbc.table.incremental.column.name", "contract_id");
    props.setProperty("hoodie.datasource.jdbc.extra.options", "fetchsize=1000,timestampFormat=yyyy-mm-dd hh:mm:ss");
  }

  private void clear(TypedProperties props) {
    props.clear();
  }

  private void writeSecretToFs() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    FSDataOutputStream outputStream = fs.create(new Path(fullPathToSecret));
    outputStream.writeBytes(secretString);
    outputStream.close();
  }

  public void cleanDerby() {
    try {
      Files.walk(Paths.get(derbyDir))
          .sorted(Comparator.reverseOrder())
          .map(java.nio.file.Path::toFile)
          .forEach(File::delete);
    } catch (Exception e) {
      //close silently
      return;
    }
  }

  public void insertToDerby() {

  }

  public Connection getConnection() throws SQLException {
    if (connection == null) {
      connection = DriverManager.getConnection(derbyUrl);
      return connection;
    }
    return connection;
  }

  public void closeAllJdbcResources() {
    try {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
