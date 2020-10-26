package org.apache.hudi.hive;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionValueExtractorTest {
  @Test
  public void testHourPartition() {
    SlashEncodedHourPartitionValueExtractor hourPartition = new SlashEncodedHourPartitionValueExtractor();
    List<String> list = new ArrayList<>();
    list.add("2020-12-20-01");
    assertEquals(hourPartition.extractPartitionValuesInPath("2020/12/20/01"), list);
  }
}