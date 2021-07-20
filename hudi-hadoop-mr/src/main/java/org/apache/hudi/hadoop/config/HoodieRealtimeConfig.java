package org.apache.hudi.hadoop.config;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

public class HoodieRealtimeConfig extends HoodieConfig {

    public static final ConfigProperty<String> COMPACTION_MEMORY_FRACTION_PROP = ConfigProperty
        .key("compaction.memory.fraction")
        .defaultValue("0.75")
        .withDocumentation("Fraction of mapper/reducer task memory used for compaction of log files");

    public static final ConfigProperty<String> COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP = ConfigProperty
        .key("compaction.lazy.block.read.enabled")
        .defaultValue("true")
        .withDocumentation("used to choose a trade off between IO vs Memory when performing compaction process. "
            + "Depending on outputfile size and memory provided, "
            + "choose true to avoid OOM for large file size + small memory");

    public static final ConfigProperty<String> MAX_DFS_STREAM_BUFFER_SIZE_PROP = ConfigProperty
        .key("hoodie.memory.dfs.buffer.max.size")
        .defaultValue("1048576")
        .withDocumentation("Property to set the max memory for dfs inputstream buffer size");

    public static final ConfigProperty<String> SPILLABLE_MAP_BASE_PATH_PROP = ConfigProperty
        .key("hoodie.memory.spillable.map.path")
        .defaultValue("/tmp/")
        .withDocumentation("Property to set file path prefix for spillable file");
}
