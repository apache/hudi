package com.uber.hoodie.config;

import static org.junit.Assert.*;

import com.google.common.collect.Maps;
import com.uber.hoodie.config.HoodieWriteConfig.Builder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

public class HoodieWriteConfigTest {
  @Test
  public void testPropertyLoading() throws IOException {
    Builder builder = HoodieWriteConfig.newBuilder().withPath("/tmp");
    Map<String, String> params = Maps.newHashMap();
    params.put(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP, "5");
    params.put(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP, "2");
    ByteArrayOutputStream outStream = saveParamsIntoOutputStream(params);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outStream.toByteArray());
    try {
      builder = builder.fromInputStream(inputStream);
    } finally {
      outStream.close();
      inputStream.close();
    }
    HoodieWriteConfig config = builder.build();
    assertEquals(config.getMaxCommitsToKeep(), 5);
    assertEquals(config.getMinCommitsToKeep(), 2);
}

  private ByteArrayOutputStream saveParamsIntoOutputStream(Map<String, String> params) throws IOException {
    Properties properties = new Properties();
    properties.putAll(params);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    properties.store(outStream, "Saved on " + new Date(System.currentTimeMillis()));
    return outStream;
  }
}