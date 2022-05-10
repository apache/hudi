package org.apache.hudi.client.transaction;

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieLockException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;

public class TestFileSystemBasedLockProvider extends HoodieCommonTestHarness {

    private static LockConfiguration lockConfiguration;
    private static Properties properties;

    @BeforeEach
    public void init() throws IOException {
        initMetaClient();
        properties = new Properties();
        properties.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
        properties.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "1000");
        properties.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "15");
        properties.setProperty(LOCK_ACQUIRE_EXPIRE_PROP_KEY, "5");
        lockConfiguration = new LockConfiguration(properties);
        properties.setProperty("hoodie.base.path", this.metaClient.getBasePath());
        properties.setProperty("hoodie.client.heartbeat.interval_in_ms", "60000");
    }

    @Test
    public void testAcquireLock() {
        HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
        FileSystemBasedLockProvider fileSystemBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
        Assertions.assertTrue(fileSystemBasedLockProvider.tryLockWithInstant(lockConfiguration.getConfig()
                .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, "123"));
        fileSystemBasedLockProvider.unlock();
    }

    @Test
    public void testUnLock() {
        HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
        FileSystemBasedLockProvider fileSystemBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
        Assertions.assertTrue(fileSystemBasedLockProvider.tryLock(lockConfiguration.getConfig()
                .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
        fileSystemBasedLockProvider.unlock();
        fileSystemBasedLockProvider.tryLock(lockConfiguration.getConfig()
                .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS);
    }

    @Test
    public void testReentrantLock() {
        HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
        FileSystemBasedLockProvider fileSystemBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
        Assertions.assertTrue(fileSystemBasedLockProvider.tryLock(lockConfiguration.getConfig()
                .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
        try {
            boolean lockHold = fileSystemBasedLockProvider.tryLock(lockConfiguration.getConfig()
                    .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS);
            Assertions.assertFalse(lockHold);
        } catch (HoodieLockException e) {
        }
        fileSystemBasedLockProvider.unlock();
    }

    @Test
    public void testReentrantLockWithExpire() {
        HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
        FileSystemBasedLockProvider fileSystemBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
        String timestamp = String.valueOf(System.currentTimeMillis());
        Assertions.assertTrue(fileSystemBasedLockProvider.tryLockWithInstant(lockConfiguration.getConfig()
                .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, timestamp));
        try {
            boolean lockHold = fileSystemBasedLockProvider.tryLockWithInstant(lockConfiguration.getConfig()
                    .getLong(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS, timestamp);
            Assertions.assertTrue(lockHold);
        } catch (HoodieLockException e) {
        }
        fileSystemBasedLockProvider.unlock();
    }

    @Test
    public void testUnlockWithoutLock() {
        HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(properties).build();
        FileSystemBasedLockProvider fileSystemBasedLockProvider = new FileSystemBasedLockProvider(lockConfiguration, writeConfig, this.metaClient.getHadoopConf());
        fileSystemBasedLockProvider.unlock();
    }

}
