package org.apache.hudi.utilities.inline.fs;

import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.utilities.inline.fs.FileSystemTestUtils.*;

public class TestParquetReadWriteFlow {

  private final Configuration inMemoryConf;
  private final Configuration inlineConf;

  public TestParquetReadWriteFlow() {
    inMemoryConf = new Configuration();
    inMemoryConf.set("fs." + InMemoryFileSystem.SCHEME + ".impl", InMemoryFileSystem.class.getName());
    inlineConf = new Configuration();
    inlineConf.set("fs." + InlineFileSystem.SCHEME + ".impl", InlineFileSystem.class.getName());
  }

  @Test
  public void testSimpleInlineFileSystem() throws IOException {
    Path outerInMemFSPath = getRandomOuterInMemPath();
    Path outerPath = new Path(FILE_SCHEME + outerInMemFSPath.toString().substring(outerInMemFSPath.toString().indexOf(':')));

    ParquetWriter inlineWriter = new AvroParquetWriter(outerInMemFSPath, HoodieTestDataGenerator.avroSchema,
        CompressionCodecName.GZIP, 100 * 1024 * 1024, 1024 * 1024, true, inMemoryConf);
    // write few records
    List<GenericRecord> recordsToWrite = getParquetHoodieRecords();
    for (GenericRecord rec : recordsToWrite) {
      inlineWriter.write(rec);
    }
    inlineWriter.close();
    byte[] inlineBytes = getBytesToInline(outerInMemFSPath);
    long startOffset = generateOuterFile(outerPath, inlineBytes);

    long inlineLength = inlineBytes.length;

    // Generate phantom inline file
    Path inlinePath = getPhantomFile(outerPath, startOffset, inlineLength);

    // instantiate Parquet reader
    ParquetReader inLineReader = AvroParquetReader.builder(inlinePath).withConf(inlineConf).build();
    List<GenericRecord> records = readParquetGenericRecords(inLineReader);
    Assert.assertArrayEquals(recordsToWrite.toArray(), records.toArray());
    inLineReader.close();
  }

  private long generateOuterFile(Path outerPath, byte[] inlineBytes) throws IOException {
    FSDataOutputStream wrappedOut = outerPath.getFileSystem(inMemoryConf).create(outerPath, true);
    // write random bytes
    writeRandomBytes(wrappedOut, 10);

    // save position for start offset
    long startOffset = wrappedOut.getPos();
    // embed inline file
    wrappedOut.write(inlineBytes);

    // write random bytes
    writeRandomBytes(wrappedOut, 5);
    wrappedOut.hsync();
    wrappedOut.close();
    return startOffset;
  }

  private byte[] getBytesToInline(Path outerInMemFSPath) throws IOException {
    InMemoryFileSystem inMemoryFileSystem = (InMemoryFileSystem) outerInMemFSPath.getFileSystem(inMemoryConf);
    return inMemoryFileSystem.getFileAsBytes();
  }

  static List<GenericRecord> readParquetGenericRecords(ParquetReader reader) throws IOException {
    List<GenericRecord> toReturn = new ArrayList<>();
    Object obj = reader.read();
    while (obj instanceof GenericRecord) {
      toReturn.add((GenericRecord) obj);
      obj = reader.read();
    }
    return toReturn;
  }

  private void writeRandomBytes(FSDataOutputStream writer, int count) throws IOException {
    for (int i = 0; i < count; i++) {
      writer.writeUTF(UUID.randomUUID().toString());
    }
  }

  static List<GenericRecord> getParquetHoodieRecords() throws IOException {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    String commitTime = "001";
    List<HoodieRecord> hoodieRecords = dataGenerator.generateInsertsWithHoodieAvroPayload(commitTime, 10);
    List<GenericRecord> toReturn = new ArrayList<>();
    for (HoodieRecord record : hoodieRecords) {
      toReturn.add((GenericRecord) record.getData().getInsertValue(HoodieTestDataGenerator.avroSchema).get());
    }
    return toReturn;
  }
}
