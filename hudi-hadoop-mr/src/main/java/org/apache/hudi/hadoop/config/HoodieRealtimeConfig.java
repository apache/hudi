package org.apache.hudi.hadoop.config;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

public class HoodieRealtimeConfig extends HoodieConfig {

    public static final ConfigProperty<Double> COMPACTION_MEMORY_FRACTION_PROP = ConfigProperty
        .key("compaction.memory.fraction")
        .defaultValue(0.75)
        .withDocumentation("Fraction of mapper/reducer task memory used for compaction of log files");

    public static final ConfigProperty<Boolean> COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP = ConfigProperty
        .key("compaction.lazy.block.read.enabled")
        .defaultValue(true)
        .withDocumentation("used to choose a trade off between IO vs Memory when performing compaction process. "
            + "Depending on outputfile size and memory provided, "
            + "choose true to avoid OOM for large file size + small memory");
}
