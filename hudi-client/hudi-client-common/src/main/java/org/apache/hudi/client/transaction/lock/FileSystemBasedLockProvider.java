package org.apache.hudi.client.transaction.lock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieLockException;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;

public class FileSystemBasedLockProvider implements LockProvider<String>, Serializable {
    private static final String LOCK = "lock";

    private final int retryMaxCount;
    private final int retryWaitTimeMs;
    private final int expireTimeSec;
    private final long heartbeatInterval;
    private transient FileSystem fs;
    private transient Path lockFile;
    private transient String basePath;
    protected LockConfiguration lockConfiguration;

    public FileSystemBasedLockProvider(final LockConfiguration lockConfiguration, final HoodieWriteConfig writeConfig, final Configuration configuration) {
        this.lockConfiguration = lockConfiguration;
        this.retryWaitTimeMs = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY);
        this.retryMaxCount = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY);
        this.expireTimeSec = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_EXPIRE_PROP_KEY);
        this.heartbeatInterval = writeConfig.getHoodieClientHeartbeatIntervalInMs();
        this.basePath = writeConfig.getBasePath();
        this.lockFile = new Path(writeConfig.getBasePath() + "/.hoodie/" + LOCK);
        this.fs = FSUtils.getFs(this.lockFile.toString(), configuration);
    }

    @Override
    public void close() {
        synchronized (LOCK) {
            try {
                fs.delete(this.lockFile, true);
            } catch (IOException e) {
                throw new HoodieLockException("Unable to release lock: " + getLock(), e);
            }
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        try {
            int numRetries = 0;
            synchronized (LOCK) {
                while (fs.exists(this.lockFile)) {
                    LOCK.wait(retryWaitTimeMs);
                    numRetries++;
                    if (numRetries > retryMaxCount) {
                        return false;
                    }
                }
                acquireLock();
                return fs.exists(this.lockFile);
            }
        } catch (IOException | InterruptedException e) {
            throw new HoodieLockException("Failed to acquire lock: " + getLock(), e);
        }
    }

    @Override
    public boolean tryLockWithInstant(long time, TimeUnit unit, String timestamp) {
        try {
            int numRetries = 0;
            synchronized (LOCK) {
                while (fs.exists(this.lockFile)) {
                    LOCK.wait(retryWaitTimeMs);
                    numRetries++;
                    if (numRetries > retryMaxCount) {
                        return false;
                    }
                    if(isLockExpire()){
                        fs.delete(this.lockFile, true);
                    }
                }
                acquireLock(timestamp);
                return fs.exists(this.lockFile);
            }
        } catch (IOException | InterruptedException e) {
            throw new HoodieLockException("Failed to acquire lock: " + getLock(), e);
        }
    }

    @Override
    public void unlock() {
        synchronized (LOCK) {
            try {
                if (fs.exists(this.lockFile)) {
                    fs.delete(this.lockFile, true);
                }
            } catch (IOException io) {
                throw new HoodieIOException("Unable to delete lock " + getLock() + "on disk", io);
            }
        }
    }

    @Override
    public String getLock() {
        return this.lockFile.toString();
    }

    private void acquireLock() {
        try {
            fs.create(this.lockFile, false);
        } catch (IOException e) {
            throw new HoodieIOException("Failed to acquire lock: " + getLock(), e);
        }
    }

    /**
     * acquire lock, then set commit time and lock acquire time
     * @param timestamp
     */
    private void acquireLock(String timestamp) {
        try {
            FSDataOutputStream stream = fs.create(this.lockFile, false);
            stream.writeLong(Long.parseLong(timestamp));
            stream.writeLong(System.currentTimeMillis());
            stream.close();
        } catch (IOException e) {
            throw new HoodieIOException("Failed to acquire lock: " + getLock(), e);
        }
    }

    private boolean isLockExpire() {
        try {
            FSDataInputStream stream = fs.open(lockFile);
            long instantTime = stream.readLong();
            Long lastHeartbeatTime = HoodieHeartbeatClient.getLastHeartbeatTime(this.fs, this.basePath, String.valueOf(instantTime));

            // if the heartbeat of this instant already expired, check if lock is expire
            if (System.currentTimeMillis() - lastHeartbeatTime > this.heartbeatInterval + 1000) {
                long createTime = stream.readLong();
                if (System.currentTimeMillis() - createTime > this.expireTimeSec * 1000) {
                    return true;
                }
            }
        } catch (IOException e) {
        }
        return false;
    }
}
