package org.apache.hudi.sync.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestConfigUtils {

  @Test
  public void testToMapSucceeds() {
    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("k.1.1.2", "v1");
    expectedMap.put("k.2.1.2", "v2");
    expectedMap.put("k.3.1.2", "v3");

    // Test base case
    String srcKv = "k.1.1.2=v1\nk.2.1.2=v2\nk.3.1.2=v3";
    Map<String, String> outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test ends with new line
    srcKv = "k.1.1.2=v1\nk.2.1.2=v2\nk.3.1.2=v3\n";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test delimited by multiple new lines
    srcKv = "k.1.1.2=v1\nk.2.1.2=v2\n\nk.3.1.2=v3";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test delimited by multiple new lines with spaces in between
    srcKv = "k.1.1.2=v1\n  \nk.2.1.2=v2\n\nk.3.1.2=v3";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);

    // Test with random spaces if trim works properly
    srcKv = " k.1.1.2 =   v1\n k.2.1.2 = v2 \nk.3.1.2 = v3";
    outMap = ConfigUtils.toMap(srcKv);
    assertEquals(expectedMap, outMap);
  }

  @Test
  public void testToMapThrowError() {
    String srcKv = "k.1.1.2=v1=v1.1\nk.2.1.2=v2\nk.3.1.2=v3";
    assertThrows(IllegalArgumentException.class, () -> ConfigUtils.toMap(srcKv));
  }
}