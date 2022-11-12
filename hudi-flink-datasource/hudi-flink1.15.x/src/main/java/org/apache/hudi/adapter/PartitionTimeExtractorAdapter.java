package org.apache.hudi.adapter;

import org.apache.flink.connector.file.table.DefaultPartTimeExtractor;

import javax.annotation.Nullable;

/**
 * Bridge class for flink partition time extractor {@code PartitionTimeExtractor}.
 */
public class PartitionTimeExtractorAdapter extends DefaultPartTimeExtractor {
  public PartitionTimeExtractorAdapter(@Nullable String extractorPattern, @Nullable String formatterPattern) {
    super(extractorPattern, formatterPattern);
  }

}
