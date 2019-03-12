package com.uber.hoodie.bench.reader;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;

import com.uber.hoodie.bench.utils.TestUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.utilities.UtilitiesTestBase;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDFSAvroDeltaInputReader extends UtilitiesTestBase {

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
  }

  @Test
  public void testDFSSinkReader() throws IOException {
    FileSystem fs = FSUtils.getFs(dfsBasePath, new Configuration());
    // Create 10 avro files with 10 records each
    TestUtils.createAvroFiles(jsc, sparkSession, dfsBasePath, 10, 10);
    FileStatus[] statuses = fs.globStatus(new Path(dfsBasePath + "/*/*.avro"));
    DFSAvroDeltaInputReader reader =
        new DFSAvroDeltaInputReader(sparkSession, TestUtils.getSchema().toString(), dfsBasePath, Optional.empty(),
            Optional.empty());
    assertEquals(reader.analyzeSingleFile(statuses[0].getPath().toString()), 10);
    assertEquals(reader.read(100).count(), 100);
    assertEquals(reader.read(1000).count(), 100);
    assertEquals(reader.read(10).count(), 10);
    assertTrue(reader.read(11).count() > 11);
  }

}
