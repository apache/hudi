package org.apache.hudi.adapter;

import com.google.inject.internal.util.$Nullable;
import org.apache.flink.table.filesystem.DefaultPartTimeExtractor;

import javax.annotation.Nullable;

/**
 * Bridge class for flink partition time extractor {@code PartitionTimeExtractor}.
 */
public class PartitionTimeExtractorAdapter extends DefaultPartTimeExtractor {
  public PartitionTimeExtractorAdapter(@Nullable String extractPattern, @$Nullable String formatPattern) {
    super(extractPattern);
  }
}
