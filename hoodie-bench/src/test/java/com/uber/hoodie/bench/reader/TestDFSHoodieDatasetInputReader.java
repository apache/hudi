package com.uber.hoodie.bench.reader;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.utilities.UtilitiesTestBase;
import com.uber.hoodie.utilities.schema.FilebasedSchemaProvider;
import java.util.HashSet;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDFSHoodieDatasetInputReader extends UtilitiesTestBase {

  private FilebasedSchemaProvider schemaProvider;

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS("hoodie-bench-config/complex-source.avsc"),
        jsc);
    HoodieTestUtils.init(jsc.hadoopConfiguration(), dfsBasePath);
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
  }

  @Test
  public void testSimpleHoodieDatasetReader() throws Exception {

    HoodieWriteConfig config = makeHoodieClientConfig();
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    String commitTime = client.startCommit();
    HoodieTestDataGenerator generator = new HoodieTestDataGenerator();
    // Insert 100 records across 3 partitions
    List<HoodieRecord> inserts = generator.generateInserts(commitTime, 100);
    JavaRDD<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(inserts), commitTime);
    writeStatuses.count();

    DFSHoodieDatasetInputReader reader = new DFSHoodieDatasetInputReader(jsc, config.getBasePath(), config.getSchema());
    // Try to read 100 records for the same partition path and same file ID
    JavaRDD<GenericRecord> records = reader.read(1, 1, 100L);
    assertTrue(records.count() <= 100);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).collect()).size(),
        1);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.FILENAME_METADATA_FIELD)).collect()).size(),
        1);

    // Try to read 100 records for 3 partition paths and 3 different file ids
    records = reader.read(3, 3, 100L);
    assertTrue(records.count() <= 100);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).collect()).size(),
        3);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.FILENAME_METADATA_FIELD)).collect()).size(),
        3);

    // Try to read 100 records for 3 partition paths and 50% records from each file
    records = reader.read(3, 3, 0.5);
    assertTrue(records.count() <= 100);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).collect()).size(),
        3);
    assertEquals(new HashSet<>(records.map(p -> p.get(HoodieRecord.FILENAME_METADATA_FIELD)).collect()).size(),
        3);
  }

  private HoodieWriteConfig makeHoodieClientConfig() throws Exception {
    return makeHoodieClientConfigBuilder().build();
  }

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder() throws Exception {
    // Prepare the AvroParquetIO
    return HoodieWriteConfig.newBuilder().withPath(dfsBasePath)
        .withParallelism(2, 2)
        .withSchema(HoodieTestDataGenerator
            .TRIP_EXAMPLE_SCHEMA);
  }

}
