package org.apache.hudi.config.metrics;

import org.apache.hudi.client.transaction.lock.metrics.HoodieLockMetrics;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metrics.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;

public class TestHoodieLockMetrics {

    @TempDir
    public java.nio.file.Path tempDir;
    protected String basePath = null;

    @BeforeEach
    public void setUp() throws IOException {
        java.nio.file.Path basePath = tempDir.resolve("dataset");
        java.nio.file.Files.createDirectories(basePath);
        this.basePath = basePath.toString();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTaskManagerConcurrentRegisterMetrics(){
        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(10, new CustomizedThreadFactory("hudi-metrics-"));

        threadPoolExecutor.submit( () -> {
            HoodieWriteConfig writeConfig = getConfigBuilder(false).build();
            return new HoodieLockMetrics(writeConfig);
        });
    }
    protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {

        return HoodieWriteConfig.newBuilder().withPath(basePath)
                .withSchema(TRIP_EXAMPLE_SCHEMA)
                .withParallelism(2, 2)
                .withAutoCommit(autoCommit)
                .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
                        .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
                .withStorageConfig(HoodieStorageConfig.newBuilder()
                        .hfileMaxFileSize(1024 * 1024 * 1024).parquetMaxFileSize(1024 * 1024 * 1024).orcMaxFileSize(1024 * 1024 * 1024).build())
                .forTable("test-trip-table")
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
                        .withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE).build());
    }
}
