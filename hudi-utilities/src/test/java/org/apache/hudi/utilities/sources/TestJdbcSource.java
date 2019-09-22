package org.apache.hudi.utilities.sources;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;
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

  private final static String hdfsSecretDir = "file:///tmp/hudi/config/";
  private final static String secretFileName = "secret";
  private final static String fullPathToSecret = hdfsSecretDir + secretFileName;
  private final static String secretString = "hudi";
  private final static String derbyDatabase = "sales";
  private final static String derbyDir = String.format("/tmp/derby/%s", derbyDatabase);
  private final static String derbyUrl = String.format("jdbc:derby:%s;create=true", derbyDir);
  PreparedStatement preparedStatement;
  final HoodieTestDataGenerator generator= new HoodieTestDataGenerator();



  private static Logger LOG = LoggerFactory.getLogger(JDBCSource.class);
  private TypedProperties props = new TypedProperties();

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
    cleanDerby();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
    cleanDerby();
  }

  public static void cleanDerby()//Pair<Connection,Statement> qureyPair)
  {
    try {
      /*if(qureyPair.getLeft()!=null)
      {
        qureyPair.getLeft().close();
      }
      if(qureyPair.getRight()!=null)
      {
        qureyPair.getRight().close();
      }*/

      Files.walk(Paths.get(derbyDir))
          .sorted(Comparator.reverseOrder())
          .map(java.nio.file.Path::toFile)
          .forEach(File::delete);
    } catch (Exception e) {
      //close silently
    }
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
  }

  @Test
  public void testJDBCSourceWithSecretFile() throws IOException {
    clear(props);
    prepareProps(props);
    writeSecretToFs();
    addSecretFileToProps(props);
    Source jdbcSource = new JDBCSource(props, jsc, sparkSession, null);
    InputBatch inputBatch = jdbcSource.fetchNewData(Option.empty(), Long.MAX_VALUE);
    Dataset<Row> rowDataset = (Dataset<Row>) inputBatch.getBatch().get();
    LOG.info("Reading test output");
    rowDataset.show();
    long count = rowDataset.count();
    LOG.info("Num records in output {}", count);
    Assert.assertEquals(count, 4L);

  }

  @Test
  public void testJDBCSourceWithSerect() throws IOException {
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

  @Test
  public void prepareDB()
      throws Exception {
    try {
      final String contractDerbyTables = "CREATE TABLE contract (\n"
          + "  contract_id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
          + "  contract_type VARCHAR(3) CONSTRAINT contract_type_ck CHECK (contract_type IN ('FUT','OPT','SPR')) DEFAULT NULL,\n"
          + "  country VARCHAR(3) DEFAULT NULL,\n"
          + "  on_asset VARCHAR(500) DEFAULT NULL,\n"
          + "  contract_created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
          + "  PRIMARY KEY (contract_id)"
          + ")";
      final String preparedStatementInsertQuery = "INSERT INTO contract(contract_type,country,on_asset) values(?,?,?)";
      Connection connection = DriverManager.getConnection(derbyUrl);
      Statement statement = connection.createStatement();
      statement.executeUpdate(contractDerbyTables);
     //  = connection.prepareStatement(preparedStatementInsertQuery);
      ResultSet resultSet = statement.executeQuery("SELECT * FROM contract");
      while (resultSet.next()) {
        LOG.warn("{},{},{}", resultSet.getString(1), resultSet.getString(2), resultSet.getString(2));
      }
      List<HoodieRecord> hoodieRecords = generator.generateInserts("000", 2);
      LOG.warn(hoodieRecords.toString());

    } catch (Exception e) {
      throw e;
    }
  }

  public void cleanUp(Connection connection, Statement statement) {
    try {
      statement.executeUpdate("drop table contract");
      statement.close();

    } catch (SQLException e) {
    }
  }
}
