package org.apache.hudi.common.model;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hoodie TimelineZone.
 */
public enum HoodieTimelineZone {
    LOCAL("local"),
    UTC("utc");

    private final String timezone;

    HoodieTimelineZone(String timezone){ this.timezone = timezone; }

    public String getTimezone() {
        return timezone;
    }
}
