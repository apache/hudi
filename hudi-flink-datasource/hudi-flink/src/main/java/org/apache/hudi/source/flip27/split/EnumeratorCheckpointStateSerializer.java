package org.apache.hudi.source.flip27.split;

import org.apache.hudi.source.flip27.HoodieSourceEnumState;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class EnumeratorCheckpointStateSerializer implements SimpleVersionedSerializer<HoodieSourceEnumState> {

  private static final int VERSION = 1;

  private static final int VERSION_1_MAGIC_NUMBER = 0xDEADBEEF;

  private final SimpleVersionedSerializer<MergeOnReadInputSplit> splitSerializer;

  @Override
  public int getVersion() {
    return VERSION;
  }

  public EnumeratorCheckpointStateSerializer(SimpleVersionedSerializer<MergeOnReadInputSplit> splitSerializer) {
    this.splitSerializer = splitSerializer;
  }

  @Override
  public byte[] serialize(HoodieSourceEnumState obj) throws IOException {
    final List<String> instants = obj.getIssuedInstants();
    final List<MergeOnReadInputSplit> splits = obj.getUnassigned();
    final SimpleVersionedSerializer<MergeOnReadInputSplit> splitSerializer = this.splitSerializer;
    final ArrayList<byte[]> serInstants = new ArrayList<>(instants.size());
    final ArrayList<byte[]> serSplits = new ArrayList<>(splits.size());
    int totalLen = 16;
    for (String instant : instants) {
      final byte[] serInstant = instant.getBytes(StandardCharsets.UTF_8);
      serInstants.add(serInstant);
      totalLen += serInstant.length + 4;
    }

    for (MergeOnReadInputSplit split : splits) {
      final  byte[] serMorSplit = splitSerializer.serialize(split);
      serSplits.add(serMorSplit);
      totalLen += serMorSplit.length + 4;
    }
    final byte[] result = new byte[totalLen];
    final ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
    byteBuffer.putInt(VERSION_1_MAGIC_NUMBER);
    byteBuffer.putInt(splitSerializer.getVersion());
    byteBuffer.putInt(serInstants.size());
    byteBuffer.putInt(serSplits.size());

    for (byte[] splitBytes : serInstants) {
      byteBuffer.putInt(splitBytes.length);
      byteBuffer.put(splitBytes);
    }

    for (byte[] pathBytes : serSplits) {
      byteBuffer.putInt(pathBytes.length);
      byteBuffer.put(pathBytes);
    }

    assert byteBuffer.remaining() == 0;

    return result;


  }

  @Override
  public HoodieSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
    final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

    final int magic = bb.getInt();
    if (magic != VERSION_1_MAGIC_NUMBER) {
      throw new IOException(
        String.format(
          "Invalid magic number for PendingSplitsCheckpoint. "
            + "Expected: %X , found %X",
          VERSION_1_MAGIC_NUMBER, magic));
    }

    final int splitSerializerVersion = bb.getInt();
    final int numIssuedInstant = bb.getInt();
    final int numSplits = bb.getInt();

    final SimpleVersionedSerializer<MergeOnReadInputSplit> splitSerializer = this.splitSerializer; // stack cache
    final ArrayList<MergeOnReadInputSplit> splits = new ArrayList<>(numSplits);
    final ArrayList<String> issuedInstants = new ArrayList<>(numIssuedInstant);

    for (int remaining = numSplits; remaining > 0; remaining--) {
      final byte[] bytes = new byte[bb.getInt()];
      bb.get(bytes);
      final MergeOnReadInputSplit split = splitSerializer.deserialize(splitSerializerVersion, bytes);
      splits.add(split);
    }

    for (int remaining = numIssuedInstant; remaining > 0; remaining--) {
      final byte[] bytes = new byte[bb.getInt()];
      bb.get(bytes);
      issuedInstants.add(new String(bytes));
    }
    return new HoodieSourceEnumState(issuedInstants, splits);
  }
}
