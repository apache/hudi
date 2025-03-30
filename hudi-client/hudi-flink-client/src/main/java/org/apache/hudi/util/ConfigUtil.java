package org.apache.hudi.util;

import java.util.HashMap;
import java.util.Map;

public class ConfigUtil {
  /**
   * Collects the config options that start with specified prefix {@code prefix} into a 'key'='value' list.
   */
  public static Map<String, String> getPropertiesWithPrefix(Map<String, String> options, String prefix) {
    final Map<String, String> hoodieProperties = new HashMap<>();
    if (hasPropertyOptions(options, prefix)) {
      options.keySet().stream()
          .filter(key -> key.startsWith(prefix))
          .forEach(key -> {
            final String value = options.get(key);
            final String subKey = key.substring(prefix.length());
            hoodieProperties.put(subKey, value);
          });
    }
    return hoodieProperties;
  }

  private static boolean hasPropertyOptions(Map<String, String> options, String prefix) {
    return options.keySet().stream().anyMatch(k -> k.startsWith(prefix));
  }
}
