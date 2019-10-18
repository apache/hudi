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
import java.util.stream.IntStream;
import javax.xml.crypto.Data;
import org.apache.avro.generic.GenericRecord;
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
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.functions;

public class TestJdbcSource extends UtilitiesTestBase {

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
    DatabaseUtils.closeAllJdbcResources();
  }

  private void addSecretFileToProps(TypedProperties props) {
    props.setProperty("hoodie.datasource.jdbc.password.file", DatabaseUtils.getFullPathToSecret());
  }

  private void addSecretStringToProps(TypedProperties props) {
    props.setProperty("hoodie.datasource.jdbc.password", DatabaseUtils.getSecret());
  }


  private void prepareProps(TypedProperties props) {
    props.setProperty("hoodie.datasource.jdbc.url", DatabaseUtils.getUrl());
    props.setProperty("hoodie.datasource.jdbc.user", DatabaseUtils.getUser());
    props.setProperty("hoodie.datasource.jdbc.table.name", "trips");
    props.setProperty("hoodie.datasource.jdbc.driver.class", "org.apache.derby.jdbc.EmbeddedDriver");
    props.setProperty("hoodie.datasource.jdbc.jar.path", "");
    props.setProperty("hoodie.datasource.write.recordkey.field", "row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "driver");
    props.setProperty("hoodie.datasource.jdbc.incremental.pull", "true");
    props.setProperty("hoodie.datasource.jdbc.table.incremental.column.name", "last_insert");
    props.setProperty("hoodie.datasource.jdbc.extra.options", "fetchsize=1000");
  }

  private void clear(TypedProperties props) {
    props.clear();
  }

  private void writeSecretToFs() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    FSDataOutputStream outputStream = fs.create(new Path(DatabaseUtils.getFullPathToSecret()));
    outputStream.writeBytes(DatabaseUtils.getSecret());
    outputStream.close();
  }

  public void cleanDerby() {
    try {
      Files.walk(Paths.get(DatabaseUtils.getDerbyDir()))
          .sorted(Comparator.reverseOrder())
          .map(java.nio.file.Path::toFile)
          .forEach(File::delete);
    } catch (Exception e) {
      //exit silently
      return;
    }
  }

  @Test
  public void testOnlyInserts() {
    try {
      int recordsToInsert = 100;
      String commitTime = "000";

      //Insert records to Derby
      DatabaseUtils.clearAndInsert(commitTime, recordsToInsert);

      //Validate if we have specified records in db
      Assert.assertEquals(recordsToInsert, DatabaseUtils.count());

      //Prepare props
      clear(props);
      prepareProps(props);
      addSecretStringToProps(props);

      //Start JDBCSource
      Source jdbcSource = new JDBCSource(props, jsc, sparkSession, null);
      InputBatch inputBatch = jdbcSource.fetchNewData(Option.empty(), Long.MAX_VALUE);
      Dataset<Row> rowDataset = (Dataset<Row>) inputBatch.getBatch().get();

      //Assert if recordsToInsert are equal to the records in the DF
      long count = rowDataset.count();
      LOG.info("Num records in output {}", count);
      Assert.assertEquals(count, recordsToInsert);

    } catch (IOException | SQLException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testInsertAndUpdate() throws IOException, SQLException {
    final String commitTime = "000";
    final int recordsToInsert = 100;
    final int recordsToUpdate = 50;

    List<HoodieRecord> insert = DatabaseUtils.clearAndInsert(commitTime, recordsToInsert);
    List<HoodieRecord> update = DatabaseUtils.update(commitTime, insert);
  }

  @Test
  public void testCheckpointingWhenPipelineIsIncremental() throws IOException, SQLException {
    DatabaseUtils.clearAndInsert("000", 10);

    //Prepare props
    clear(props);
    prepareProps(props);
    addSecretStringToProps(props);

    //Start JDBCSource
    Source jdbcSource = new JDBCSource(props, jsc, sparkSession, null);
    InputBatch inputBatch = jdbcSource.fetchNewData(Option.empty(), Long.MAX_VALUE);
    Dataset<Row> rowDataset = (Dataset<Row>) inputBatch.getBatch().get();
    String incrementalVal = rowDataset.agg(functions.max(functions.col(
        props.getString("hoodie.datasource.jdbc.table.incremental.column.name"))).cast(DataTypes.StringType)).first()
        .getString(0);
    System.out.println("Incremental value is " + incrementalVal);

    DatabaseUtils.insert("000", 5);
    Source jdbcSource1 = new JDBCSource(props, jsc, sparkSession, null);
    InputBatch inputBatch1 = jdbcSource1.fetchNewData(Option.of(incrementalVal), Long.MAX_VALUE);
    Dataset<Row> rowDataset1 = (Dataset<Row>) inputBatch1.getBatch().get();
    System.out.println(rowDataset1.count());


  }


  private static class DatabaseUtils {

    private static final HoodieTestDataGenerator generator = new HoodieTestDataGenerator();
    private static final String hdfsSecretDir = "file:///tmp/hudi/config/";
    private static final String secretFileName = "secret";
    private static final String fullPathToSecret = hdfsSecretDir + secretFileName;
    private static final String secretString = "hudi123";
    private static final String user = "hudi";
    private static final String derbyDatabase = "uber";
    private static final String derbyDir = String.format("/tmp/derby/%s", derbyDatabase);
    private static final String derbyUrl = String
        .format("jdbc:derby:%s;user=%s;password=%s;create=true", derbyDir, user, secretString);
    private static final String createTrips = "CREATE TABLE trips("
        + "commit_time VARCHAR(50),"
        + "row_key VARCHAR(50),"
        + "rider VARCHAR(50),"
        + "driver VARCHAR(50),"
        + "begin_lat DOUBLE PRECISION,"
        + "begin_lon DOUBLE PRECISION,"
        + "end_lat DOUBLE PRECISION,"
        + "end_lon DOUBLE PRECISION,"
        + "fare DOUBLE PRECISION,"
        + "last_insert TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)";
    private static final String dropTrips = "DROP TABLE trips";
    private static Connection connection;
    private static PreparedStatement insertStatement;
    private static PreparedStatement updateStatement;

    public static Connection getConnection() throws SQLException {
      if (connection == null) {
        connection = DriverManager.getConnection(derbyUrl);
        return connection;
      }
      return connection;
    }

    public static void closeAllJdbcResources() {
      try {
        if (insertStatement != null) {
          insertStatement.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    private static List<HoodieRecord> insert(String commitTime, int numRecords) throws SQLException, IOException {

      insertStatement =
          insertStatement == null ? getConnection().prepareStatement("INSERT INTO trips("
              + "commit_time,"
              + "row_key,"
              + "rider,"
              + "driver,"
              + "begin_lat,"
              + "begin_lon,"
              + "end_lat,"
              + "end_lon,"
              + "fare) "
              + "values(?,?,?,?,?,?,?,?,?)")
              : insertStatement;
      List<HoodieRecord> hoodieRecords = generator.generateInserts(commitTime, numRecords);

      hoodieRecords
          .stream()
          .map(r -> {
            try {
              return ((GenericRecord) r.getData().getInsertValue(HoodieTestDataGenerator.avroSchema).get());
            } catch (IOException e) {
              return null;
            }
          })
          .filter(r -> r != null)
          .parallel()
          .forEach(record -> {
            try {
              insertStatement.setString(1, record.get("timestamp").toString());
              insertStatement.setString(2, record.get("_row_key").toString());
              insertStatement.setString(3, record.get("rider").toString());
              insertStatement.setString(4, record.get("driver").toString());
              insertStatement.setDouble(5, Double.parseDouble(record.get("begin_lat").toString()));
              insertStatement.setDouble(6, Double.parseDouble(record.get("begin_lon").toString()));
              insertStatement.setDouble(7, Double.parseDouble(record.get("end_lat").toString()));
              insertStatement.setDouble(8, Double.parseDouble(record.get("end_lon").toString()));
              insertStatement.setDouble(9, Double.parseDouble(record.get("fare").toString()));
              insertStatement.addBatch();
            } catch (SQLException e) {
              e.printStackTrace();
            }
          });
      insertStatement.executeBatch();
      return hoodieRecords;
    }

    static List<HoodieRecord> update(String commitTime, List<HoodieRecord> inserts) throws SQLException, IOException {
//      updateStatement =
//          updateStatement == null ? getConnection().prepareStatement("");

      List<HoodieRecord> updateRecords = generator.generateUpdates(commitTime, inserts);
      System.out.println(updateRecords);
      return updateRecords;
    }

    public static List<HoodieRecord> clearAndInsert(String commitTime, int numRecords)
        throws IOException, SQLException {
      execute(dropTrips, "Table does not exists");
      execute(createTrips, "Table already exists");
      return insert(commitTime, numRecords);
    }

    public static void execute(String query, String message) {
      Statement statement = null;
      try {
        statement = getConnection().createStatement();
        statement.executeUpdate(query);
      } catch (SQLException e) {
        System.out.println(message);
      } finally {
        close(statement);
      }

    }

    public static void close(Statement statement) {
      try {
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        return;
      }
    }

    public static int count() {
      Statement statement = null;
      try {
        statement = getConnection().createStatement();
        ResultSet rs = statement.executeQuery("select count(*) from trips");
        rs.next();
        int count = rs.getInt(1);
        statement.close();
        return count;
      } catch (SQLException e) {
        e.printStackTrace();
        return 0;
      }

    }

    public static String getUrl() {
      return derbyUrl;
    }

    public static String getUser() {
      return user;
    }

    public static String getFullPathToSecret() {
      return fullPathToSecret;
    }

    public static String getSecret() {
      return secretString;
    }

    public static String getDerbyDir() {
      return derbyDir;
    }
  }
}
