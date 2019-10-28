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
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJdbcSource extends UtilitiesTestBase {

  private static Logger LOG = LogManager.getLogger(JDBCSource.class);
  private TypedProperties props = new TypedProperties();

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
    DatabaseUtils.closeAllJdbcResources();
    cleanDerby();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
    DatabaseUtils.closeAllJdbcResources();
    cleanDerby();
  }

  @Before
  @Override
  public void setup() throws Exception {
    super.setup();
  }

  @After
  @Override
  public void teardown() throws Exception {
    super.teardown();
  }

  private void addSecretStringToProps(TypedProperties props) {
    props.setProperty("hoodie.datasource.jdbc.password", DatabaseUtils.getSecret());
  }


  private void prepareProps(TypedProperties props) {
    props.setProperty("hoodie.datasource.jdbc.url", DatabaseUtils.getUrl());
    props.setProperty("hoodie.datasource.jdbc.user", DatabaseUtils.getUser());
    props.setProperty("hoodie.datasource.jdbc.table.name", "trips");
    props.setProperty("hoodie.datasource.jdbc.driver.class", "org.apache.derby.jdbc.EmbeddedDriver");
    props.setProperty("hoodie.datasource.write.recordkey.field", "row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "driver");
    props.setProperty("hoodie.datasource.jdbc.incremental.pull", "true");
    props.setProperty("hoodie.datasource.jdbc.table.incremental.column.name", "last_insert");
    props.setProperty("hoodie.datasource.jdbc.extra.options.fetchsize", "1000");
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

  public static void cleanDerby() {
    try {
      Files.walk(Paths.get(DatabaseUtils.getDerbyDir()))
          .sorted(Comparator.reverseOrder())
          .map(java.nio.file.Path::toFile)
          .forEach(File::delete);
    } catch (Exception e) {
      //exit silently
      return;
    } finally {
      //Needed to avoid connection issues and shut derby
      System.gc();
    }
  }

  @Test
  public void testSingleCommit() {
    try {
      int recordsToInsert = 100;
      String commitTime = "000";

      //Insert records to Derby
      DatabaseUtils.clearAndInsert(commitTime, recordsToInsert);

      //Validate if we have specified records in db
      Assert.assertEquals(recordsToInsert, DatabaseUtils.count());

      //Prepare props
      getProps();

      //Start JDBCSource
      runSourceAndAssert(Option.empty(), recordsToInsert);

    } catch (IOException | SQLException e) {
      handle(e);
    }
  }

  private void handle(Exception e) {
    e.printStackTrace();
    Assert.fail(e.getMessage());
  }

  @Test
  public void testInsertAndUpdate() {
    try {
      getProps();
      final String commitTime = "000";
      final int recordsToInsert = 100;

      //Add 100 records
      //Update half of them with commit time "007"
      //Check if updates are through or not
      DatabaseUtils.update("007",
          DatabaseUtils.clearAndInsert(commitTime, recordsToInsert)
              .stream()
              .limit(50)
              .collect(Collectors.toList())
      );
      //Check if database has 100 records
      Assert.assertEquals("Database has 100 records", 100, DatabaseUtils.count());

      //Start JDBCSource
      Dataset<Row> rowDataset = runSourceAndAssert(Option.empty(), 100);

      Dataset<Row> firstCommit = rowDataset.where("commit_time=000");
      Assert.assertEquals("Should have 50 records with commit time 000", 50, firstCommit.count());

      Dataset<Row> secondCommit = rowDataset.where("commit_time=007");
      Assert.assertEquals("Should have 50 records with commit time 000", 50, secondCommit.count());

    } catch (Exception e) {
      handle(e);
    }
  }

  @Test
  public void testSourceWithPasswordOnFs() {
    try {
      //Write secret string to fs in a file
      writeSecretToFs();
      //Prepare props
      getProps();
      //Remove secret string from props
      props.remove("hoodie.datasource.jdbc.password");
      //Set property to read secret from fs file
      props.setProperty("hoodie.datasource.jdbc.password.file", DatabaseUtils.getFullPathToSecret());
      //Add 10 records with commit time 000
      DatabaseUtils.clearAndInsert("000", 10);
      runSourceAndAssert(Option.empty(), 10);
    } catch (Exception e) {
      handle(e);
    }
  }


  @Test(expected = NoSuchElementException.class)
  //Internally JDBCSource should throw an IllegalArgumentException as the password is not give.
  //However it is internally handled. Even so we assert NoSuchElementException which is thrown
  //from the Options class there is no Dataset computed.
  public void testSourceWithNoPassword() {
    try {
      //Write secret string to fs in a file
      writeSecretToFs();
      //Prepare props
      getProps();
      //Remove secret string from props
      props.remove("hoodie.datasource.jdbc.password");

      //Add 10 records with commit time 000
      DatabaseUtils.clearAndInsert("000", 10);
      runSourceAndAssert(Option.empty(), 10);
    } catch (IOException | SQLException e) {
      handle(e);
    }
  }


  @Test
  public void testTwoCommits() {
    try {
      //Prepare props
      getProps();

      //Add 10 records with commit time "000"
      DatabaseUtils.clearAndInsert("000", 10);

      //Start JDBCSource
      Dataset<Row> rowDataset = runSourceAndAssert(Option.empty(), 10);
      Assert.assertEquals(10, rowDataset.where("commit_time=000").count());

      //Add 10 records with commit time 001
      DatabaseUtils.insert("001", 5);
      rowDataset = runSourceAndAssert(Option.empty(), 15);
      Assert.assertEquals(5, rowDataset.where("commit_time=001").count());
      Assert.assertEquals(10, rowDataset.where("commit_time=000").count());

      //Start second commit and check if all records are pulled
      runSourceAndAssert(Option.empty(), 15);


    } catch (Exception e) {
      handle(e);
    }

  }

  private Dataset<Row> runSourceAndAssert(Option<String> option, int expected) {
    Source jdbcSource = new JDBCSource(props, jsc, sparkSession, null);
    InputBatch inputBatch = jdbcSource.fetchNewData(option, Long.MAX_VALUE);
    Dataset<Row> rowDataset = (Dataset<Row>) inputBatch.getBatch().get();
    Assert.assertEquals(expected, rowDataset.count());
    return rowDataset;
  }


  @Test
  public void testIncrementalScanWithTimestamp() {
    try {
      getProps();

      //Add 10 records with commit time "000"
      DatabaseUtils.clearAndInsert("000", 10);

      //Start JDBCSource
      Dataset<Row> rowDataset = runSourceAndAssert(Option.empty(), 10);

      //get max of incremental column
      Column incrementalColumn = rowDataset
          .col(props.getString("hoodie.datasource.jdbc.table.incremental.column.name"));
      final String max = rowDataset.agg(functions.max(incrementalColumn).cast(DataTypes.StringType)).first()
          .getString(0);
      LOG.info(String.format("Incremental max value: %s", max));

      //Add 10 records with commit time "001"
      DatabaseUtils.insert("001", 10);

      //Start Incremental scan
      //Because derby incremental scan PPQ query fails due to the timestamp issues the incremental scan should
      //throw an exception internally and fallback to a full scan. Hence validate if the all the rows of the
      //table are selected or not
      Dataset<Row> rowDataset1 = runSourceAndAssert(Option.of(max), 20);
      Assert.assertEquals(10, rowDataset1.where("commit_time=000").count());
      Assert.assertEquals(10, rowDataset1.where("commit_time=001").count());

    } catch (Exception e) {
      handle(e);
    }
  }

  private void getProps() {
    //Prepare props
    clear(props);
    prepareProps(props);
    addSecretStringToProps(props);
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
        + "id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"
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
        if (updateStatement != null) {
          updateStatement.close();
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
          .forEach(record -> {
            try {
              insertStatement.setString(1, commitTime);
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
      updateStatement =
          updateStatement == null ? getConnection().prepareStatement("UPDATE trips set commit_time=?,"
              + "row_key=?,"
              + "rider=?,"
              + "driver=?,"
              + "begin_lat=?,"
              + "begin_lon=?,"
              + "end_lat=?,"
              + "end_lon=?,"
              + "fare=?"
              + "where row_key=?") : updateStatement;

      List<HoodieRecord> updateRecords = generator.generateUpdates(commitTime, inserts);
      updateRecords.stream().map(m -> {
        try {
          return m.getData().getInsertValue(HoodieTestDataGenerator.avroSchema).get();
        } catch (IOException e) {
          return null;
        }
      }).filter(r -> r != null)
          .map(r -> ((GenericRecord) r))
          .sequential()
          .forEach(r -> {
            try {
              updateStatement.setString(1, commitTime);
              updateStatement.setString(2, r.get("_row_key").toString());
              updateStatement.setString(3, r.get("rider").toString());
              updateStatement.setString(4, r.get("driver").toString());
              updateStatement.setDouble(5, Double.parseDouble(r.get("begin_lat").toString()));
              updateStatement.setDouble(6, Double.parseDouble(r.get("begin_lon").toString()));
              updateStatement.setDouble(7, Double.parseDouble(r.get("end_lat").toString()));
              updateStatement.setDouble(8, Double.parseDouble(r.get("end_lon").toString()));
              updateStatement.setDouble(9, Double.parseDouble(r.get("fare").toString()));
              updateStatement.setString(10, r.get("_row_key").toString());
              updateStatement.addBatch();
            } catch (SQLException e) {
              LOG.warn(e);
            }
          });
      updateStatement.executeBatch();
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

    public static void close(Connection connection) {
      try {
        if (connection != null) {
          connection.close();
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

    public static void shutdown() {
      Connection shutdownConnection = null;
      try {
        String shutdownUrl = getUrl() + ";shutdown=true";
        System.out.println(String.format("Shutting down Derby DB @ %s", shutdownUrl));
        shutdownConnection = DriverManager.getConnection(shutdownUrl);
      } catch (SQLException e) {
        e.printStackTrace();
      } finally {
        close(shutdownConnection);
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
