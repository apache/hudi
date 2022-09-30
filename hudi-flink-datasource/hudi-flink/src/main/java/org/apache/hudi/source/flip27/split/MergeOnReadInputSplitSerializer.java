package org.apache.hudi.source.flip27.split;

import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 *
 */
public class MergeOnReadInputSplitSerializer implements SimpleVersionedSerializer<MergeOnReadInputSplit> {

  @Override
  public int getVersion() {
    return 1;
  }

  @Override
  public byte[] serialize(MergeOnReadInputSplit obj) throws IOException {
    return InstantiationUtil.serializeObject(obj);
  }

  @Override
  public MergeOnReadInputSplit deserialize(int version, byte[] serialized) throws IOException {
    try {
      return InstantiationUtil.deserializeObject(serialized, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
