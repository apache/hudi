package org.apache.hudi.common.table.log;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.IndexedRecord;

import java.util.List;

/**
 * Scans a set of log files to extract metadata about the log blocks. It does not read the actual records.
 */
public class HoodieLogBlockMetadataScanner extends BaseHoodieLogRecordReader<IndexedRecord> {

  public HoodieLogBlockMetadataScanner(HoodieTableMetaClient metaClient, List<String> logFilePaths, int bufferSize, Option<InstantRange> instantRange) {
    super(getReaderContext(metaClient), metaClient, metaClient.getStorage(), logFilePaths, false, bufferSize, instantRange, false, false, Option.empty(), Option.empty(), true,
        null, false);
    scanInternal(Option.empty(), true);
  }

  private static HoodieReaderContext<IndexedRecord> getReaderContext(HoodieTableMetaClient metaClient) {
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(metaClient.getStorage().getConf(), metaClient.getTableConfig());
    readerContext.setHasLogFiles(true);
    readerContext.setHasBootstrapBaseFile(false);
    readerContext.setLatestCommitTime(metaClient.getActiveTimeline().lastInstant().map(HoodieInstant::requestedTime).orElseThrow(() -> new IllegalStateException("No completed instant found")));
    return readerContext;
  }
}
