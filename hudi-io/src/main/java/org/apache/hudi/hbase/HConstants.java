/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hbase;

import static org.apache.hudi.hbase.io.hfile.BlockType.MAGIC_LENGTH;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hudi.hbase.util.Bytes;

/**
 * HConstants holds a bunch of HBase-related constants
 */
@InterfaceAudience.Public
public final class HConstants {
  // NOTICE!!!! Please do not add a constants here, unless they are referenced by a lot of classes.

  //Bytes.UTF8_ENCODING should be updated if this changed
  /** When we encode strings, we always specify UTF8 encoding */
  public static final String UTF8_ENCODING = "UTF-8";

  //Bytes.UTF8_CHARSET should be updated if this changed
  /** When we encode strings, we always specify UTF8 encoding */
  public static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);
  /**
   * Default block size for an HFile.
   */
  public final static int DEFAULT_BLOCKSIZE = 64 * 1024;

  /** Used as a magic return value while optimized index key feature enabled(HBASE-7845) */
  public final static int INDEX_KEY_MAGIC = -2;

  /*
   * Name of directory that holds recovered edits written by the wal log
   * splitting code, one per region
   */
  public static final String RECOVERED_EDITS_DIR = "recovered.edits";

  /*
   * Name of directory that holds recovered hfiles written by the wal log
   * splitting code, one per region
   */
  public static final String RECOVERED_HFILES_DIR = "recovered.hfiles";

  /**
   * Date Tiered Compaction tmp dir prefix name if use storage policy
   */
  public static final String STORAGE_POLICY_PREFIX = "storage_policy_";

  /**
   * The first four bytes of Hadoop RPC connections
   */
  public static final byte[] RPC_HEADER = new byte[] { 'H', 'B', 'a', 's' };
  public static final byte RPC_CURRENT_VERSION = 0;

  // HFileBlock constants. TODO!!!! THESE DEFINES BELONG IN HFILEBLOCK, NOT UP HERE.
  // Needed down in hbase-common though by encoders but these encoders should not be dealing
  // in the internals of hfileblocks. Fix encapsulation.

  /** The size data structures with minor version is 0 */
  public static final int HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM = MAGIC_LENGTH + 2 * Bytes.SIZEOF_INT
      + Bytes.SIZEOF_LONG;
  /** The size of a version 2 HFile block header, minor version 1.
   * There is a 1 byte checksum type, followed by a 4 byte bytesPerChecksum
   * followed by another 4 byte value to store sizeofDataOnDisk.
   */
  public static final int HFILEBLOCK_HEADER_SIZE = HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM +
      Bytes.SIZEOF_BYTE + 2 * Bytes.SIZEOF_INT;
  /** Just an array of bytes of the right size. */
  public static final byte[] HFILEBLOCK_DUMMY_HEADER = new byte[HFILEBLOCK_HEADER_SIZE];

  //End HFileBlockConstants.

  /**
   * Status codes used for return values of bulk operations.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
  public enum OperationStatusCode {
    NOT_RUN,
    SUCCESS,
    BAD_FAMILY,
    STORE_TOO_BUSY,
    SANITY_CHECK_FAILURE,
    FAILURE
  }

  /** long constant for zero */
  public static final Long ZERO_L = Long.valueOf(0L);
  public static final String NINES = "99999999999999";
  public static final String ZEROES = "00000000000000";

  // For migration

  /** name of version file */
  public static final String VERSION_FILE_NAME = "hbase.version";

  /**
   * Current version of file system.
   * Version 4 supports only one kind of bloom filter.
   * Version 5 changes versions in catalog table regions.
   * Version 6 enables blockcaching on catalog tables.
   * Version 7 introduces hfile -- hbase 0.19 to 0.20..
   * Version 8 introduces namespace
   */
  // public static final String FILE_SYSTEM_VERSION = "6";
  public static final String FILE_SYSTEM_VERSION = "8";

  // Configuration parameters

  //TODO: Is having HBase homed on port 60k OK?

  /** Cluster is in distributed mode or not */
  public static final String CLUSTER_DISTRIBUTED = "hbase.cluster.distributed";

  /** Config for pluggable load balancers */
  public static final String HBASE_MASTER_LOADBALANCER_CLASS = "hbase.master.loadbalancer.class";

  /** Config for balancing the cluster by table */
  public static final String HBASE_MASTER_LOADBALANCE_BYTABLE = "hbase.master.loadbalance.bytable";

  /** Config for the max percent of regions in transition */
  public static final String HBASE_MASTER_BALANCER_MAX_RIT_PERCENT =
      "hbase.master.balancer.maxRitPercent";

  /** Default value for the max percent of regions in transition */
  public static final double DEFAULT_HBASE_MASTER_BALANCER_MAX_RIT_PERCENT = 1.0;

  /** Config for the max balancing time */
  public static final String HBASE_BALANCER_MAX_BALANCING = "hbase.balancer.max.balancing";

  /** Config for the balancer period */
  public static final String HBASE_BALANCER_PERIOD = "hbase.balancer.period";

  /** Default value for the balancer period */
  public static final int DEFAULT_HBASE_BALANCER_PERIOD = 300000;

  /**
   * Config key for enable/disable automatically separate child regions to different region servers
   * in the procedure of split regions. One child will be kept to the server where parent
   * region is on, and the other child will be assigned to a random server.
   * See HBASE-25518.
   */
  public static final String HBASE_ENABLE_SEPARATE_CHILD_REGIONS =
      "hbase.master.auto.separate.child.regions.after.split.enabled";

  /**
   * Default value for automatically separate child regions to different region servers
   * (set to "false" to keep all child regions to the server where parent region is on)
   */
  public static final boolean DEFAULT_HBASE_ENABLE_SEPARATE_CHILD_REGIONS = false;

  /** The name of the ensemble table */
  public static final TableName ENSEMBLE_TABLE_NAME = TableName.valueOf("hbase:ensemble");

  /** Config for pluggable region normalizer */
  public static final String HBASE_MASTER_NORMALIZER_CLASS =
      "hbase.master.normalizer.class";

  /** Cluster is standalone or pseudo-distributed */
  public static final boolean CLUSTER_IS_LOCAL = false;

  /** Cluster is fully-distributed */
  @Deprecated // unused. see HBASE-13636. remove this in 3.0
  public static final boolean CLUSTER_IS_DISTRIBUTED = true;

  /** Default value for cluster distributed mode */
  public static final boolean DEFAULT_CLUSTER_DISTRIBUTED = CLUSTER_IS_LOCAL;

  /** default host address */
  public static final String DEFAULT_HOST = "0.0.0.0";

  /** Parameter name for port master listens on. */
  public static final String MASTER_PORT = "hbase.master.port";

  /** default port that the master listens on */
  public static final int DEFAULT_MASTER_PORT = 16000;

  /** default port for master web api */
  public static final int DEFAULT_MASTER_INFOPORT = 16010;

  /** Configuration key for master web API port */
  public static final String MASTER_INFO_PORT = "hbase.master.info.port";

  /** Configuration key for the list of master host:ports **/
  public static final String MASTER_ADDRS_KEY = "hbase.masters";

  /** Full class name of the Zookeeper based connection registry implementation */
  public static final String ZK_CONNECTION_REGISTRY_CLASS =
      "org.apache.hudi.hbase.client.ZKConnectionRegistry";

  /** Parameter name for the master type being backup (waits for primary to go inactive). */
  public static final String MASTER_TYPE_BACKUP = "hbase.master.backup";

  /**
   * by default every master is a possible primary master unless the conf explicitly overrides it
   */
  public static final boolean DEFAULT_MASTER_TYPE_BACKUP = false;

  /** Name of ZooKeeper quorum configuration parameter. */
  public static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

  /** Name of ZooKeeper quorum configuration parameter for client to locate meta. */
  public static final String CLIENT_ZOOKEEPER_QUORUM = "hbase.client.zookeeper.quorum";

  /** Client port of ZooKeeper for client to locate meta */
  public static final String CLIENT_ZOOKEEPER_CLIENT_PORT =
      "hbase.client.zookeeper.property.clientPort";

  /** Indicate whether the client ZK are observer nodes of the server ZK */
  public static final String CLIENT_ZOOKEEPER_OBSERVER_MODE =
      "hbase.client.zookeeper.observer.mode";
  /** Assuming client zk not in observer mode and master need to synchronize information */
  public static final boolean DEFAULT_CLIENT_ZOOKEEPER_OBSERVER_MODE = false;

  /** Common prefix of ZooKeeper configuration properties */
  public static final String ZK_CFG_PROPERTY_PREFIX =
      "hbase.zookeeper.property.";

  public static final int ZK_CFG_PROPERTY_PREFIX_LEN =
      ZK_CFG_PROPERTY_PREFIX.length();

  /**
   * The ZK client port key in the ZK properties map. The name reflects the
   * fact that this is not an HBase configuration key.
   */
  public static final String CLIENT_PORT_STR = "clientPort";

  /** Parameter name for the client port that the zookeeper listens on */
  public static final String ZOOKEEPER_CLIENT_PORT =
      ZK_CFG_PROPERTY_PREFIX + CLIENT_PORT_STR;

  /**
   * Will be removed in hbase 3.0
   * @deprecated use {@link #DEFAULT_ZOOKEEPER_CLIENT_PORT} instead
   */
  @Deprecated
  public static final int DEFAULT_ZOOKEPER_CLIENT_PORT = 2181;

  /** Default client port that the zookeeper listens on */
  public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;

  /**
   * Parameter name for the wait time for the recoverable zookeeper
   */
  @Deprecated // unused. see HBASE-3065. remove this in 3.0
  public static final String ZOOKEEPER_RECOVERABLE_WAITTIME =
      "hbase.zookeeper.recoverable.waittime";

  /** Default wait time for the recoverable zookeeper */
  @Deprecated // unused. see HBASE-3065. remove this in 3.0
  public static final long DEFAULT_ZOOKEPER_RECOVERABLE_WAITIME = 10000;

  /** Parameter name for the root dir in ZK for this cluster */
  public static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/hbase";

  /**
   * Parameter name for the limit on concurrent client-side zookeeper
   * connections
   */
  public static final String ZOOKEEPER_MAX_CLIENT_CNXNS =
      ZK_CFG_PROPERTY_PREFIX + "maxClientCnxns";

  /** Parameter name for the ZK data directory */
  public static final String ZOOKEEPER_DATA_DIR =
      ZK_CFG_PROPERTY_PREFIX + "dataDir";

  /** Parameter name for the ZK tick time */
  public static final String ZOOKEEPER_TICK_TIME =
      ZK_CFG_PROPERTY_PREFIX + "tickTime";

  /**
   * Will be removed in hbase 3.0
   * @deprecated use {@link #DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS} instead
   */
  @Deprecated
  public static final int DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS = 300;

  /** Default limit on concurrent client-side zookeeper connections */
  public static final int DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS = 300;

  /** Configuration key for ZooKeeper session timeout */
  public static final String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout";

  /** Timeout for the ZK sync() call */
  public static final String ZK_SYNC_BLOCKING_TIMEOUT_MS = "hbase.zookeeper.sync.timeout.millis";
  // Choice of the default value is based on the following ZK recommendation (from docs). Keeping it
  // lower lets the callers fail fast in case of any issues.
  // "The clients view of the system is guaranteed to be up-to-date within a certain time bound.
  // (On the order of tens of seconds.) Either system changes will be seen by a client within this
  // bound, or the client will detect a service outage."
  public static final long ZK_SYNC_BLOCKING_TIMEOUT_DEFAULT_MS = 30 * 1000;

  /** Default value for ZooKeeper session timeout */
  public static final int DEFAULT_ZK_SESSION_TIMEOUT = 90 * 1000;

  /** Parameter name for port region server listens on. */
  public static final String REGIONSERVER_PORT = "hbase.regionserver.port";

  /** Default port region server listens on. */
  public static final int DEFAULT_REGIONSERVER_PORT = 16020;

  /** default port for region server web api */
  public static final int DEFAULT_REGIONSERVER_INFOPORT = 16030;

  /** A configuration key for regionserver info port */
  public static final String REGIONSERVER_INFO_PORT =
      "hbase.regionserver.info.port";

  /** A flag that enables automatic selection of regionserver info port */
  public static final String REGIONSERVER_INFO_PORT_AUTO =
      REGIONSERVER_INFO_PORT + ".auto";

  /** Parameter name for what region server implementation to use. */
  public static final String REGION_SERVER_IMPL= "hbase.regionserver.impl";

  /** Parameter name for what master implementation to use. */
  public static final String MASTER_IMPL= "hbase.master.impl";

  /** Parameter name for what hbase client implementation to use. */
  @Deprecated // unused. see HBASE-7460. remove this in 3.0
  public static final String HBASECLIENT_IMPL= "hbase.hbaseclient.impl";

  /** Parameter name for how often threads should wake up */
  public static final String THREAD_WAKE_FREQUENCY = "hbase.server.thread.wakefrequency";

  /** Default value for thread wake frequency */
  public static final int DEFAULT_THREAD_WAKE_FREQUENCY = 10 * 1000;

  /** Parameter name for how often we should try to write a version file, before failing */
  public static final String VERSION_FILE_WRITE_ATTEMPTS = "hbase.server.versionfile.writeattempts";

  /** Parameter name for how often we should try to write a version file, before failing */
  public static final int DEFAULT_VERSION_FILE_WRITE_ATTEMPTS = 3;

  /** Parameter name and default value for how often a region should perform a major compaction */
  public static final String MAJOR_COMPACTION_PERIOD = "hbase.hregion.majorcompaction";
  public static final long   DEFAULT_MAJOR_COMPACTION_PERIOD = 1000 * 60 * 60 * 24 * 7; // 7 days

  /**
   * Parameter name and default value for major compaction jitter.
   * Used as a multiplier applied to {@link HConstants#MAJOR_COMPACTION_PERIOD}
   * to cause compaction to occur a given amount of time either side of
   * {@link HConstants#MAJOR_COMPACTION_PERIOD}.
   * Default to 0.5 so jitter has us fall evenly either side of when the compaction should run.
   */
  public static final String MAJOR_COMPACTION_JITTER = "hbase.hregion.majorcompaction.jitter";
  public static final float  DEFAULT_MAJOR_COMPACTION_JITTER = 0.50F;

  /** Parameter name for the maximum batch of KVs to be used in flushes and compactions */
  public static final String COMPACTION_KV_MAX = "hbase.hstore.compaction.kv.max";
  public static final int COMPACTION_KV_MAX_DEFAULT = 10;

  /** Parameter name for HBase instance root directory */
  public static final String HBASE_DIR = "hbase.rootdir";

  /** Parameter name for HBase client IPC pool type */
  public static final String HBASE_CLIENT_IPC_POOL_TYPE = "hbase.client.ipc.pool.type";

  /** Parameter name for HBase client IPC pool size */
  public static final String HBASE_CLIENT_IPC_POOL_SIZE = "hbase.client.ipc.pool.size";

  /** Parameter name for HBase client operation timeout. */
  public static final String HBASE_CLIENT_OPERATION_TIMEOUT = "hbase.client.operation.timeout";

  /** Parameter name for HBase client meta operation timeout. */
  public static final String HBASE_CLIENT_META_OPERATION_TIMEOUT =
      "hbase.client.meta.operation.timeout";

  /** Default HBase client operation timeout, which is tantamount to a blocking call */
  public static final int DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT = 1200000;

  /** Parameter name for HBase client meta replica scan call timeout. */
  public static final String HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT =
      "hbase.client.meta.replica.scan.timeout";

  /** Default HBase client meta replica scan call timeout, 1 second */
  public static final int HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT_DEFAULT = 1000000;

  /** Used to construct the name of the log directory for a region server */
  public static final String HREGION_LOGDIR_NAME = "WALs";

  /** Used to construct the name of the splitlog directory for a region server */
  public static final String SPLIT_LOGDIR_NAME = "splitWAL";

  /** Like the previous, but for old logs that are about to be deleted */
  public static final String HREGION_OLDLOGDIR_NAME = "oldWALs";

  /** Staging dir used by bulk load */
  public static final String BULKLOAD_STAGING_DIR_NAME = "staging";

  public static final String CORRUPT_DIR_NAME = "corrupt";

  /** Used by HBCK to sideline backup data */
  public static final String HBCK_SIDELINEDIR_NAME = ".hbck";

  /** Any artifacts left from migration can be moved here */
  public static final String MIGRATION_NAME = ".migration";

  /**
   * The directory from which co-processor/custom filter jars can be loaded
   * dynamically by the region servers. This value can be overridden by the
   * hbase.dynamic.jars.dir config.
   */
  @Deprecated // unused. see HBASE-12054. remove this in 3.0
  public static final String LIB_DIR = "lib";

  /** Used to construct the name of the compaction directory during compaction */
  public static final String HREGION_COMPACTIONDIR_NAME = "compaction.dir";

  /** Conf key for the max file size after which we split the region */
  public static final String HREGION_MAX_FILESIZE =
      "hbase.hregion.max.filesize";

  /** Default maximum file size */
  public static final long DEFAULT_MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024L;

  /** Conf key for if we should sum overall region files size when check to split */
  public static final String OVERALL_HREGION_FILES =
      "hbase.hregion.split.overallfiles";

  /** Default overall region files */
  public static final boolean DEFAULT_OVERALL_HREGION_FILES = false;

  /**
   * Max size of single row for Get's or Scan's without in-row scanning flag set.
   */
  public static final String TABLE_MAX_ROWSIZE_KEY = "hbase.table.max.rowsize";

  /**
   * Default max row size (1 Gb).
   */
  public static final long TABLE_MAX_ROWSIZE_DEFAULT = 1024 * 1024 * 1024L;

  /**
   * The max number of threads used for opening and closing stores or store
   * files in parallel
   */
  public static final String HSTORE_OPEN_AND_CLOSE_THREADS_MAX =
      "hbase.hstore.open.and.close.threads.max";

  /**
   * The default number for the max number of threads used for opening and
   * closing stores or store files in parallel
   */
  public static final int DEFAULT_HSTORE_OPEN_AND_CLOSE_THREADS_MAX = 1;

  /**
   * Block updates if memstore has hbase.hregion.memstore.block.multiplier
   * times hbase.hregion.memstore.flush.size bytes.  Useful preventing
   * runaway memstore during spikes in update traffic.
   */
  public static final String HREGION_MEMSTORE_BLOCK_MULTIPLIER =
      "hbase.hregion.memstore.block.multiplier";

  /**
   * Default value for hbase.hregion.memstore.block.multiplier
   */
  public static final int DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER = 4;

  /** Conf key for the memstore size at which we flush the memstore */
  public static final String HREGION_MEMSTORE_FLUSH_SIZE =
      "hbase.hregion.memstore.flush.size";

  public static final String HREGION_EDITS_REPLAY_SKIP_ERRORS =
      "hbase.hregion.edits.replay.skip.errors";

  public static final boolean DEFAULT_HREGION_EDITS_REPLAY_SKIP_ERRORS =
      false;

  /** Maximum value length, enforced on KeyValue construction */
  public static final int MAXIMUM_VALUE_LENGTH = Integer.MAX_VALUE - 1;

  /** name of the file for unique cluster ID */
  public static final String CLUSTER_ID_FILE_NAME = "hbase.id";

  /** Default value for cluster ID */
  public static final String CLUSTER_ID_DEFAULT = "default-cluster";

  /** Parameter name for # days to keep MVCC values during a major compaction */
  public static final String KEEP_SEQID_PERIOD = "hbase.hstore.compaction.keep.seqId.period";
  /** At least to keep MVCC values in hfiles for 5 days */
  public static final int MIN_KEEP_SEQID_PERIOD = 5;

  // Always store the location of the root table's HRegion.
  // This HRegion is never split.

  // region name = table + startkey + regionid. This is the row key.
  // each row in the root and meta tables describes exactly 1 region
  // Do we ever need to know all the information that we are storing?

  // Note that the name of the root table starts with "-" and the name of the
  // meta table starts with "." Why? it's a trick. It turns out that when we
  // store region names in memory, we use a SortedMap. Since "-" sorts before
  // "." (and since no other table name can start with either of these
  // characters, the root region will always be the first entry in such a Map,
  // followed by all the meta regions (which will be ordered by their starting
  // row key as well), followed by all user tables. So when the Master is
  // choosing regions to assign, it will always choose the root region first,
  // followed by the meta regions, followed by user regions. Since the root
  // and meta regions always need to be on-line, this ensures that they will
  // be the first to be reassigned if the server(s) they are being served by
  // should go down.

  public static final String BASE_NAMESPACE_DIR = "data";

  /** delimiter used between portions of a region name */
  public static final int META_ROW_DELIMITER = ',';

  /** The catalog family as a string*/
  public static final String CATALOG_FAMILY_STR = "info";

  /** The catalog family */
  public static final byte [] CATALOG_FAMILY = Bytes.toBytes(CATALOG_FAMILY_STR);

  /** The RegionInfo qualifier as a string */
  public static final String REGIONINFO_QUALIFIER_STR = "regioninfo";

  /** The regioninfo column qualifier */
  public static final byte [] REGIONINFO_QUALIFIER = Bytes.toBytes(REGIONINFO_QUALIFIER_STR);

  /** The server column qualifier */
  public static final String SERVER_QUALIFIER_STR = "server";
  /** The server column qualifier */
  public static final byte [] SERVER_QUALIFIER = Bytes.toBytes(SERVER_QUALIFIER_STR);

  /** The startcode column qualifier */
  public static final String STARTCODE_QUALIFIER_STR = "serverstartcode";
  /** The startcode column qualifier */
  public static final byte [] STARTCODE_QUALIFIER = Bytes.toBytes(STARTCODE_QUALIFIER_STR);

  /** The open seqnum column qualifier */
  public static final String SEQNUM_QUALIFIER_STR = "seqnumDuringOpen";
  /** The open seqnum column qualifier */
  public static final byte [] SEQNUM_QUALIFIER = Bytes.toBytes(SEQNUM_QUALIFIER_STR);

  /** The state column qualifier */
  public static final String STATE_QUALIFIER_STR = "state";

  public static final byte [] STATE_QUALIFIER = Bytes.toBytes(STATE_QUALIFIER_STR);

  /**
   * The serverName column qualifier. Its the server where the region is
   * transitioning on, while column server is the server where the region is
   * opened on. They are the same when the region is in state OPEN.
   */
  public static final String SERVERNAME_QUALIFIER_STR = "sn";

  public static final byte [] SERVERNAME_QUALIFIER = Bytes.toBytes(SERVERNAME_QUALIFIER_STR);

  /** The lower-half split region column qualifier string. */
  public static final String SPLITA_QUALIFIER_STR = "splitA";
  /** The lower-half split region column qualifier */
  public static final byte [] SPLITA_QUALIFIER = Bytes.toBytes(SPLITA_QUALIFIER_STR);

  /** The upper-half split region column qualifier String. */
  public static final String SPLITB_QUALIFIER_STR = "splitB";
  /** The upper-half split region column qualifier */
  public static final byte [] SPLITB_QUALIFIER = Bytes.toBytes(SPLITB_QUALIFIER_STR);

  /**
   * Merge qualifier prefix.
   * We used to only allow two regions merge; mergeA and mergeB.
   * Now we allow many to merge. Each region to merge will be referenced
   * in a column whose qualifier starts with this define.
   */
  public static final String MERGE_QUALIFIER_PREFIX_STR = "merge";
  public static final byte [] MERGE_QUALIFIER_PREFIX =
      Bytes.toBytes(MERGE_QUALIFIER_PREFIX_STR);

  /**
   * The lower-half merge region column qualifier
   * @deprecated Since 2.3.0 and 2.2.1. Not used anymore. Instead we look for
   *   the {@link #MERGE_QUALIFIER_PREFIX_STR} prefix.
   */
  @Deprecated
  public static final byte[] MERGEA_QUALIFIER = Bytes.toBytes(MERGE_QUALIFIER_PREFIX_STR + "A");

  /**
   * The upper-half merge region column qualifier
   * @deprecated Since 2.3.0 and 2.2.1. Not used anymore. Instead we look for
   *   the {@link #MERGE_QUALIFIER_PREFIX_STR} prefix.
   */
  @Deprecated
  public static final byte[] MERGEB_QUALIFIER = Bytes.toBytes(MERGE_QUALIFIER_PREFIX_STR + "B");

  /** The catalog family as a string*/
  public static final String TABLE_FAMILY_STR = "table";

  /** The catalog family */
  public static final byte [] TABLE_FAMILY = Bytes.toBytes(TABLE_FAMILY_STR);

  /** The serialized table state qualifier */
  public static final byte[] TABLE_STATE_QUALIFIER = Bytes.toBytes("state");

  /** The replication barrier family as a string*/
  public static final String REPLICATION_BARRIER_FAMILY_STR = "rep_barrier";

  /** The replication barrier family */
  public static final byte[] REPLICATION_BARRIER_FAMILY =
      Bytes.toBytes(REPLICATION_BARRIER_FAMILY_STR);

  /**
   * The meta table version column qualifier.
   * We keep current version of the meta table in this column in <code>-ROOT-</code>
   * table: i.e. in the 'info:v' column.
   */
  public static final byte [] META_VERSION_QUALIFIER = Bytes.toBytes("v");

  /** The family str as a key in map*/
  public static final String FAMILY_KEY_STR = "family";

  /**
   * The current version of the meta table.
   * - pre-hbase 0.92.  There is no META_VERSION column in the root table
   * in this case. The meta has HTableDescriptor serialized into the HRegionInfo;
   * - version 0 is 0.92 and 0.94. Meta data has serialized HRegionInfo's using
   * Writable serialization, and HRegionInfo's does not contain HTableDescriptors.
   * - version 1 for 0.96+ keeps HRegionInfo data structures, but changes the
   * byte[] serialization from Writables to Protobuf.
   * See HRegionInfo.VERSION
   */
  public static final short META_VERSION = 1;

  // Other constants

  /**
   * An empty byte array instance.
   */
  public static final byte [] EMPTY_BYTE_ARRAY = new byte [0];

  /**
   * An empty string instance.
   */
  public static final String EMPTY_STRING = "";

  public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);

  /**
   * Used by scanners, etc when they want to start at the beginning of a region
   */
  public static final byte [] EMPTY_START_ROW = EMPTY_BYTE_ARRAY;

  /**
   * Last row in a table.
   */
  public static final byte [] EMPTY_END_ROW = EMPTY_BYTE_ARRAY;

  /**
   * Used by scanners and others when they're trying to detect the end of a
   * table
   */
  public static final byte [] LAST_ROW = EMPTY_BYTE_ARRAY;

  /**
   * Max length a row can have because of the limitation in TFile.
   */
  public static final int MAX_ROW_LENGTH = Short.MAX_VALUE;

  /**
   * Timestamp to use when we want to refer to the latest cell.
   * This is the timestamp sent by clients when no timestamp is specified on
   * commit.
   */
  public static final long LATEST_TIMESTAMP = Long.MAX_VALUE;

  /**
   * Timestamp to use when we want to refer to the oldest cell.
   * Special! Used in fake Cells only. Should never be the timestamp on an actual Cell returned to
   * a client.
   * @deprecated Should not be public since hbase-1.3.0. For internal use only. Move internal to
   *   Scanners flagged as special timestamp value never to be returned as timestamp on a Cell.
   */
  @Deprecated
  public static final long OLDEST_TIMESTAMP = Long.MIN_VALUE;

  /**
   * LATEST_TIMESTAMP in bytes form
   */
  public static final byte [] LATEST_TIMESTAMP_BYTES = {
      // big-endian
      (byte) (LATEST_TIMESTAMP >>> 56),
      (byte) (LATEST_TIMESTAMP >>> 48),
      (byte) (LATEST_TIMESTAMP >>> 40),
      (byte) (LATEST_TIMESTAMP >>> 32),
      (byte) (LATEST_TIMESTAMP >>> 24),
      (byte) (LATEST_TIMESTAMP >>> 16),
      (byte) (LATEST_TIMESTAMP >>> 8),
      (byte) LATEST_TIMESTAMP,
  };

  /**
   * Define for 'return-all-versions'.
   */
  public static final int ALL_VERSIONS = Integer.MAX_VALUE;

  /**
   * Unlimited time-to-live.
   */
//  public static final int FOREVER = -1;
  public static final int FOREVER = Integer.MAX_VALUE;

  /**
   * Seconds in a week
   */
  @Deprecated // unused. see HBASE-2692. remove this in 3.0
  public static final int WEEK_IN_SECONDS = 7 * 24 * 3600;

  /**
   * Seconds in a day, hour and minute
   */
  public static final int DAY_IN_SECONDS = 24 * 60 * 60;
  public static final int HOUR_IN_SECONDS = 60 * 60;
  public static final int MINUTE_IN_SECONDS = 60;

  //TODO: although the following are referenced widely to format strings for
  //      the shell. They really aren't a part of the public API. It would be
  //      nice if we could put them somewhere where they did not need to be
  //      public. They could have package visibility
  public static final String NAME = "NAME";
  public static final String VERSIONS = "VERSIONS";
  public static final String IN_MEMORY = "IN_MEMORY";
  public static final String METADATA = "METADATA";
  public static final String CONFIGURATION = "CONFIGURATION";

  /**
   * Retrying we multiply hbase.client.pause setting by what we have in this array until we
   * run out of array items.  Retries beyond this use the last number in the array.  So, for
   * example, if hbase.client.pause is 1 second, and maximum retries count
   * hbase.client.retries.number is 10, we will retry at the following intervals:
   * 1, 2, 3, 5, 10, 20, 40, 100, 100, 100.
   * With 100ms, a back-off of 200 means 20s
   */
  public static final int [] RETRY_BACKOFF = {1, 2, 3, 5, 10, 20, 40, 100, 100, 100, 100, 200, 200};

  public static final String REGION_IMPL = "hbase.hregion.impl";

  /**
   * Scope tag for locally scoped data.
   * This data will not be replicated.
   */
  public static final int REPLICATION_SCOPE_LOCAL = 0;

  /**
   * Scope tag for globally scoped data.
   * This data will be replicated to all peers.
   */
  public static final int REPLICATION_SCOPE_GLOBAL = 1;

  /**
   * Default cluster ID, cannot be used to identify a cluster so a key with
   * this value means it wasn't meant for replication.
   */
  public static final UUID DEFAULT_CLUSTER_ID = new UUID(0L,0L);

  /**
   * Parameter name for maximum number of bytes returned when calling a scanner's next method.
   * Controlled by the client.
   */
  public static final String HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY =
      "hbase.client.scanner.max.result.size";

  /**
   * Parameter name for maximum number of bytes returned when calling a scanner's next method.
   * Controlled by the server.
   */
  public static final String HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY =
      "hbase.server.scanner.max.result.size";

  /**
   * Maximum number of bytes returned when calling a scanner's next method.
   * Note that when a single row is larger than this limit the row is still
   * returned completely.
   *
   * The default value is 2MB.
   */
  public static final long DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE = 2 * 1024 * 1024;

  /**
   * Maximum number of bytes returned when calling a scanner's next method.
   * Note that when a single row is larger than this limit the row is still
   * returned completely.
   * Safety setting to protect the region server.
   *
   * The default value is 100MB. (a client would rarely request larger chunks on purpose)
   */
  public static final long DEFAULT_HBASE_SERVER_SCANNER_MAX_RESULT_SIZE = 100 * 1024 * 1024;

  /**
   * Parameter name for client pause value, used mostly as value to wait
   * before running a retry of a failed get, region lookup, etc.
   */
  public static final String HBASE_CLIENT_PAUSE = "hbase.client.pause";

  /**
   * Default value of {@link #HBASE_CLIENT_PAUSE}.
   */
  public static final long DEFAULT_HBASE_CLIENT_PAUSE = 100;

  /**
   * Parameter name for client pause value for special case such as call queue too big, etc.
   */
  public static final String HBASE_CLIENT_PAUSE_FOR_CQTBE = "hbase.client.pause.cqtbe";

  /**
   * The maximum number of concurrent connections the client will maintain.
   */
  public static final String HBASE_CLIENT_MAX_TOTAL_TASKS = "hbase.client.max.total.tasks";

  /**
   * Default value of {@link #HBASE_CLIENT_MAX_TOTAL_TASKS}.
   */
  public static final int DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS = 100;

  /**
   * The maximum number of concurrent connections the client will maintain to a single
   * RegionServer.
   */
  public static final String HBASE_CLIENT_MAX_PERSERVER_TASKS = "hbase.client.max.perserver.tasks";

  /**
   * Default value of {@link #HBASE_CLIENT_MAX_PERSERVER_TASKS}.
   */
  public static final int DEFAULT_HBASE_CLIENT_MAX_PERSERVER_TASKS = 2;

  /**
   * The maximum number of concurrent connections the client will maintain to a single
   * Region.
   */
  public static final String HBASE_CLIENT_MAX_PERREGION_TASKS = "hbase.client.max.perregion.tasks";

  /**
   * Default value of {@link #HBASE_CLIENT_MAX_PERREGION_TASKS}.
   */
  public static final int DEFAULT_HBASE_CLIENT_MAX_PERREGION_TASKS = 1;

  /**
   * The maximum number of concurrent pending RPC requests for one server in process level.
   */
  public static final String HBASE_CLIENT_PERSERVER_REQUESTS_THRESHOLD =
      "hbase.client.perserver.requests.threshold";

  /**
   * Default value of {@link #HBASE_CLIENT_PERSERVER_REQUESTS_THRESHOLD}.
   */
  public static final int DEFAULT_HBASE_CLIENT_PERSERVER_REQUESTS_THRESHOLD = Integer.MAX_VALUE;


  /**
   * Parameter name for server pause value, used mostly as value to wait before
   * running a retry of a failed operation.
   */
  public static final String HBASE_SERVER_PAUSE = "hbase.server.pause";

  /**
   * Default value of {@link #HBASE_SERVER_PAUSE}.
   */
  public static final int DEFAULT_HBASE_SERVER_PAUSE = 1000;

  /**
   * Parameter name for maximum retries, used as maximum for all retryable
   * operations such as fetching of the root region from root region server,
   * getting a cell's value, starting a row update, etc.
   */
  public static final String HBASE_CLIENT_RETRIES_NUMBER = "hbase.client.retries.number";

  /**
   * Default value of {@link #HBASE_CLIENT_RETRIES_NUMBER}.
   */
  public static final int DEFAULT_HBASE_CLIENT_RETRIES_NUMBER = 15;

  public static final String HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER =
      "hbase.client.serverside.retries.multiplier";

  public static final int DEFAULT_HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER = 3;

  /**
   * Parameter name to set the default scanner caching for all clients.
   */
  public static final String HBASE_CLIENT_SCANNER_CACHING = "hbase.client.scanner.caching";

  /**
   * Default value for {@link #HBASE_CLIENT_SCANNER_CACHING}
   */
  public static final int DEFAULT_HBASE_CLIENT_SCANNER_CACHING = Integer.MAX_VALUE;

  /**
   * Parameter name for number of rows that will be fetched when calling next on
   * a scanner if it is not served from memory. Higher caching values will
   * enable faster scanners but will eat up more memory and some calls of next
   * may take longer and longer times when the cache is empty.
   */
  public static final String HBASE_META_SCANNER_CACHING = "hbase.meta.scanner.caching";

  /**
   * Default value of {@link #HBASE_META_SCANNER_CACHING}.
   */
  public static final int DEFAULT_HBASE_META_SCANNER_CACHING = 100;

  /**
   * Parameter name for number of versions, kept by meta table.
   */
  public static final String HBASE_META_VERSIONS = "hbase.meta.versions";

  /**
   * Default value of {@link #HBASE_META_VERSIONS}.
   */
  public static final int DEFAULT_HBASE_META_VERSIONS = 3;

  /**
   * Parameter name for number of versions, kept by meta table.
   */
  public static final String HBASE_META_BLOCK_SIZE = "hbase.meta.blocksize";

  /**
   * Default value of {@link #HBASE_META_BLOCK_SIZE}.
   */
  public static final int DEFAULT_HBASE_META_BLOCK_SIZE = 8 * 1024;

  /**
   * Parameter name for unique identifier for this {@link org.apache.hadoop.conf.Configuration}
   * instance. If there are two or more {@link org.apache.hadoop.conf.Configuration} instances that,
   * for all intents and purposes, are the same except for their instance ids, then they will not be
   * able to share the same org.apache.hudi.hbase.client.HConnection instance. On the other hand,
   * even if the instance ids are the same, it could result in non-shared
   * org.apache.hudi.hbase.client.HConnection instances if some of the other connection parameters
   * differ.
   */
  public static final String HBASE_CLIENT_INSTANCE_ID = "hbase.client.instance.id";

  /**
   * The client scanner timeout period in milliseconds.
   */
  public static final String HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD =
      "hbase.client.scanner.timeout.period";

  /**
   * Use {@link #HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD} instead.
   * @deprecated This config option is deprecated. Will be removed at later releases after 0.96.
   */
  @Deprecated
  public static final String HBASE_REGIONSERVER_LEASE_PERIOD_KEY =
      "hbase.regionserver.lease.period";

  /**
   * Default value of {@link #HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD}.
   */
  public static final int DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = 60000;

  /**
   * timeout for each RPC
   */
  public static final String HBASE_RPC_TIMEOUT_KEY = "hbase.rpc.timeout";

  /**
   * timeout for each read RPC
   */
  public static final String HBASE_RPC_READ_TIMEOUT_KEY = "hbase.rpc.read.timeout";

  /**
   * timeout for each write RPC
   */
  public static final String HBASE_RPC_WRITE_TIMEOUT_KEY = "hbase.rpc.write.timeout";

  /**
   * Default value of {@link #HBASE_RPC_TIMEOUT_KEY}
   */
  public static final int DEFAULT_HBASE_RPC_TIMEOUT = 60000;

  /**
   * timeout for short operation RPC
   */
  public static final String HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY =
      "hbase.rpc.shortoperation.timeout";

  /**
   * Default value of {@link #HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY}
   */
  public static final int DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT = 10000;

  /**
   * Value indicating the server name was saved with no sequence number.
   */
  public static final long NO_SEQNUM = -1;

  /**
   * Registry implementation to be used on the client side.
   */
  public static final String CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY =
      "hbase.client.registry.impl";

  /*
   * cluster replication constants.
   */
  public static final String
      REPLICATION_SOURCE_SERVICE_CLASSNAME = "hbase.replication.source.service";
  public static final String
      REPLICATION_SINK_SERVICE_CLASSNAME = "hbase.replication.sink.service";
  public static final String REPLICATION_SERVICE_CLASSNAME_DEFAULT =
      "org.apache.hudi.hbase.replication.regionserver.Replication";
  public static final String REPLICATION_BULKLOAD_ENABLE_KEY = "hbase.replication.bulkload.enabled";
  public static final boolean REPLICATION_BULKLOAD_ENABLE_DEFAULT = false;
  /** Replication cluster id of source cluster which uniquely identifies itself with peer cluster */
  public static final String REPLICATION_CLUSTER_ID = "hbase.replication.cluster.id";
  /**
   * Max total size of buffered entries in all replication peers. It will prevent server getting
   * OOM if there are many peers. Default value is 256MB which is four times to default
   * replication.source.size.capacity.
   */
  public static final String REPLICATION_SOURCE_TOTAL_BUFFER_KEY = "replication.total.buffer.quota";

  public static final int REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT = 256 * 1024 * 1024;

  /** Configuration key for ReplicationSource shipeEdits timeout */
  public static final String REPLICATION_SOURCE_SHIPEDITS_TIMEOUT =
      "replication.source.shipedits.timeout";
  public static final int REPLICATION_SOURCE_SHIPEDITS_TIMEOUT_DFAULT = 60000;

  /**
   * Directory where the source cluster file system client configuration are placed which is used by
   * sink cluster to copy HFiles from source cluster file system
   */
  public static final String REPLICATION_CONF_DIR = "hbase.replication.conf.dir";

  /** Maximum time to retry for a failed bulk load request */
  public static final String BULKLOAD_MAX_RETRIES_NUMBER = "hbase.bulkload.retries.number";

  /** HBCK special code name used as server name when manipulating ZK nodes */
  @Deprecated // unused. see HBASE-3789. remove this in 3.0
  public static final String HBCK_CODE_NAME = "HBCKServerName";

  public static final String KEY_FOR_HOSTNAME_SEEN_BY_MASTER =
      "hbase.regionserver.hostname.seen.by.master";

  public static final String HBASE_MASTER_LOGCLEANER_PLUGINS =
      "hbase.master.logcleaner.plugins";

  public static final String HBASE_REGION_SPLIT_POLICY_KEY =
      "hbase.regionserver.region.split.policy";

  /** Whether nonces are enabled; default is true. */
  public static final String HBASE_RS_NONCES_ENABLED = "hbase.regionserver.nonces.enabled";

  /**
   * Configuration key for the size of the block cache
   */
  public static final String HFILE_BLOCK_CACHE_SIZE_KEY =
      "hfile.block.cache.size";

  public static final float HFILE_BLOCK_CACHE_SIZE_DEFAULT = 0.4f;

  /**
   * Configuration key for setting the fix size of the block size, default do nothing and it should
   * be explicitly set by user or only used within ClientSideRegionScanner. if it's set less than
   * current max on heap size, it overrides the max size of block cache
   */
  public static final String HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY =
      "hfile.onheap.block.cache.fixed.size";
  public static final long HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_DEFAULT = 0L;
  public static final long HBASE_CLIENT_SCANNER_ONHEAP_BLOCK_CACHE_FIXED_SIZE_DEFAULT =
      32 * 1024 * 1024L;

  /*
   * Minimum percentage of free heap necessary for a successful cluster startup.
   */
  public static final float HBASE_CLUSTER_MINIMUM_MEMORY_THRESHOLD = 0.2f;

  /**
   * @deprecated  It is used internally. As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static final Pattern CP_HTD_ATTR_KEY_PATTERN =
      Pattern.compile("^coprocessor\\$([0-9]+)$", Pattern.CASE_INSENSITIVE);

  /**
   * <pre>
   * Pattern that matches a coprocessor specification. Form is:
   * {@code <coprocessor jar file location> '|' <class name> ['|' <priority> ['|' <arguments>]]}
   * where arguments are {@code <KEY> '=' <VALUE> [,...]}
   * For example: {@code hdfs:///foo.jar|com.foo.FooRegionObserver|1001|arg1=1,arg2=2}
   * </pre>
   * @deprecated  It is used internally. As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static final Pattern CP_HTD_ATTR_VALUE_PATTERN =
      Pattern.compile("(^[^\\|]*)\\|([^\\|]+)\\|[\\s]*([\\d]*)[\\s]*(\\|.*)?$");
  /**
   * @deprecated  It is used internally. As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static final String CP_HTD_ATTR_VALUE_PARAM_KEY_PATTERN = "[^=,]+";
  /**
   * @deprecated  It is used internally. As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static final String CP_HTD_ATTR_VALUE_PARAM_VALUE_PATTERN = "[^,]+";
  /**
   * @deprecated  It is used internally. As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static final Pattern CP_HTD_ATTR_VALUE_PARAM_PATTERN = Pattern.compile(
      "(" + CP_HTD_ATTR_VALUE_PARAM_KEY_PATTERN + ")=(" +
          CP_HTD_ATTR_VALUE_PARAM_VALUE_PATTERN + "),?");
  public static final String CP_HTD_ATTR_INCLUSION_KEY =
      "hbase.coprocessor.classloader.included.classes";

  /** The delay when re-trying a socket operation in a loop (HBASE-4712) */
  public static final int SOCKET_RETRY_WAIT_MS = 200;

  /** Host name of the local machine */
  public static final String LOCALHOST = "localhost";

  /**
   * If this parameter is set to true, then hbase will read
   * data and then verify checksums. Checksum verification
   * inside hdfs will be switched off.  However, if the hbase-checksum
   * verification fails, then it will switch back to using
   * hdfs checksums for verifiying data that is being read from storage.
   *
   * If this parameter is set to false, then hbase will not
   * verify any checksums, instead it will depend on checksum verification
   * being done in the hdfs client.
   */
  public static final String HBASE_CHECKSUM_VERIFICATION =
      "hbase.regionserver.checksum.verify";

  public static final String LOCALHOST_IP = "127.0.0.1";

  public static final String REGION_SERVER_HANDLER_COUNT = "hbase.regionserver.handler.count";
  public static final int DEFAULT_REGION_SERVER_HANDLER_COUNT = 30;

  /*
   * REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT:
   * -1  => Disable aborting
   * 0   => Abort if even a single handler has died
   * 0.x => Abort only when this percent of handlers have died
   * 1   => Abort only all of the handers have died
   */
  public static final String REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT =
      "hbase.regionserver.handler.abort.on.error.percent";
  public static final double DEFAULT_REGION_SERVER_HANDLER_ABORT_ON_ERROR_PERCENT = 0.5;

  //High priority handlers to deal with admin requests and system table operation requests
  public static final String REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT =
      "hbase.regionserver.metahandler.count";
  public static final int DEFAULT_REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT = 20;

  public static final String REGION_SERVER_REPLICATION_HANDLER_COUNT =
      "hbase.regionserver.replication.handler.count";
  public static final int DEFAULT_REGION_SERVER_REPLICATION_HANDLER_COUNT = 3;
  // Meta Transition handlers to deal with meta ReportRegionStateTransitionRequest. Meta transition
  // should be dealt with in a separate handler in case blocking other region's transition.
  public static final String MASTER_META_TRANSITION_HANDLER_COUNT =
      "hbase.master.meta.transition.handler.count";
  public static final int MASTER__META_TRANSITION_HANDLER_COUNT_DEFAULT = 1;

  @Deprecated // unused. see HBASE-10569. remove this in 3.0
  public static final String MASTER_HANDLER_COUNT = "hbase.master.handler.count";
  @Deprecated // unused. see HBASE-10569. remove this in 3.0
  public static final int DEFAULT_MASTER_HANLDER_COUNT = 25;

  /** Conf key that specifies timeout value to wait for a region ready */
  @Deprecated // unused. see HBASE-13616. remove this in 3.0
  public static final String LOG_REPLAY_WAIT_REGION_TIMEOUT =
      "hbase.master.log.replay.wait.region.timeout";

  /** Conf key for enabling meta replication */
  public static final String USE_META_REPLICAS = "hbase.meta.replicas.use";
  public static final boolean DEFAULT_USE_META_REPLICAS = false;

  /**
   * @deprecated Since 2.4.0, will be removed in 4.0.0. Please change the meta replicas number by
   *             altering meta table, i.e, set a new 'region replication' number and call
   *             modifyTable.
   */
  @Deprecated
  public static final String META_REPLICAS_NUM = "hbase.meta.replica.count";
  /**
   * @deprecated Since 2.4.0, will be removed in 4.0.0. Please change the meta replicas number by
   *             altering meta table, i.e, set a new 'region replication' number and call
   *             modifyTable.
   */
  @Deprecated
  public static final int DEFAULT_META_REPLICA_NUM = 1;

  /**
   * The name of the configuration parameter that specifies
   * the number of bytes in a newly created checksum chunk.
   */
  public static final String BYTES_PER_CHECKSUM =
      "hbase.hstore.bytes.per.checksum";

  /**
   * The name of the configuration parameter that specifies
   * the name of an algorithm that is used to compute checksums
   * for newly created blocks.
   */
  public static final String CHECKSUM_TYPE_NAME =
      "hbase.hstore.checksum.algorithm";

  /** Enable file permission modification from standard hbase */
  public static final String ENABLE_DATA_FILE_UMASK = "hbase.data.umask.enable";
  /** File permission umask to use when creating hbase data files */
  public static final String DATA_FILE_UMASK_KEY = "hbase.data.umask";

  /** Configuration name of WAL Compression */
  public static final String ENABLE_WAL_COMPRESSION =
      "hbase.regionserver.wal.enablecompression";

  /** Configuration name of WAL storage policy
   * Valid values are: HOT, COLD, WARM, ALL_SSD, ONE_SSD, LAZY_PERSIST
   * See http://hadoop.apache.org/docs/r2.7.3/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html*/
  public static final String WAL_STORAGE_POLICY = "hbase.wal.storage.policy";
  /**
   * "NONE" is not a valid storage policy and means we defer the policy to HDFS. @see
   * <a href="https://issues.apache.org/jira/browse/HBASE-20691">HBASE-20691</a>
   */
  public static final String DEFER_TO_HDFS_STORAGE_POLICY = "NONE";
  /** By default we defer the WAL storage policy to HDFS */
  public static final String DEFAULT_WAL_STORAGE_POLICY = DEFER_TO_HDFS_STORAGE_POLICY;

  /** Region in Transition metrics threshold time */
  public static final String METRICS_RIT_STUCK_WARNING_THRESHOLD =
      "hbase.metrics.rit.stuck.warning.threshold";

  public static final String LOAD_BALANCER_SLOP_KEY = "hbase.regions.slop";

  /** delimiter used between portions of a region name */
  public static final int DELIMITER = ',';

  /**
   * QOS attributes: these attributes are used to demarcate RPC call processing
   * by different set of handlers. For example, HIGH_QOS tagged methods are
   * handled by high priority handlers.
   */
  // normal_QOS < replication_QOS < replay_QOS < QOS_threshold < admin_QOS < high_QOS < meta_QOS
  public static final int PRIORITY_UNSET = -1;
  public static final int NORMAL_QOS = 0;
  public static final int REPLICATION_QOS = 5;
  public static final int REPLAY_QOS = 6;
  public static final int QOS_THRESHOLD = 10;
  public static final int ADMIN_QOS = 100;
  public static final int HIGH_QOS = 200;
  public static final int SYSTEMTABLE_QOS = HIGH_QOS;
  /**
   * @deprecated the name "META_QOS" is a bit ambiguous, actually only meta region transition can
   *             use this priority, and you should not use this directly. Will be removed in 3.0.0.
   */
  @Deprecated
  public static final int META_QOS = 300;

  /** Directory under /hbase where archived hfiles are stored */
  public static final String HFILE_ARCHIVE_DIRECTORY = "archive";

  /**
   * Name of the directory to store all snapshots. See SnapshotDescriptionUtils for
   * remaining snapshot constants; this is here to keep HConstants dependencies at a minimum and
   * uni-directional.
   */
  public static final String SNAPSHOT_DIR_NAME = ".hbase-snapshot";

  /* Name of old snapshot directory. See HBASE-8352 for details on why it needs to be renamed */
  public static final String OLD_SNAPSHOT_DIR_NAME = ".snapshot";

  /** Temporary directory used for table creation and deletion */
  public static final String HBASE_TEMP_DIRECTORY = ".tmp";
  /**
   * The period (in milliseconds) between computing region server point in time metrics
   */
  public static final String REGIONSERVER_METRICS_PERIOD = "hbase.regionserver.metrics.period";
  public static final long DEFAULT_REGIONSERVER_METRICS_PERIOD = 5000;
  /** Directories that are not HBase table directories */
  public static final List<String> HBASE_NON_TABLE_DIRS =
      Collections.unmodifiableList(Arrays.asList(new String[] {
          HBCK_SIDELINEDIR_NAME, HBASE_TEMP_DIRECTORY, MIGRATION_NAME
      }));

  /**
   * Directories that are not HBase user table directories.
   * @deprecated Since hbase-2.3.0; no replacement as not used any more (internally at least)
   */
  @Deprecated
  public static final List<String> HBASE_NON_USER_TABLE_DIRS =
      Collections.unmodifiableList(Arrays.asList((String[])ArrayUtils.addAll(
          new String[] { TableName.META_TABLE_NAME.getNameAsString() },
          HBASE_NON_TABLE_DIRS.toArray())));

  /** Health script related settings. */
  public static final String HEALTH_SCRIPT_LOC = "hbase.node.health.script.location";
  public static final String HEALTH_SCRIPT_TIMEOUT = "hbase.node.health.script.timeout";
  public static final String HEALTH_CHORE_WAKE_FREQ =
      "hbase.node.health.script.frequency";
  public static final long DEFAULT_HEALTH_SCRIPT_TIMEOUT = 60000;
  /**
   * The maximum number of health check failures a server can encounter consecutively.
   */
  public static final String HEALTH_FAILURE_THRESHOLD =
      "hbase.node.health.failure.threshold";
  public static final int DEFAULT_HEALTH_FAILURE_THRESHOLD = 3;


  /**
   * Setting to activate, or not, the publication of the status by the master. Default
   *  notification is by a multicast message.
   */
  public static final String STATUS_PUBLISHED = "hbase.status.published";
  public static final boolean STATUS_PUBLISHED_DEFAULT = false;

  /**
   * IP to use for the multicast status messages between the master and the clients.
   * The default address is chosen as one among others within the ones suitable for multicast
   * messages.
   */
  public static final String STATUS_MULTICAST_ADDRESS = "hbase.status.multicast.address.ip";
  public static final String DEFAULT_STATUS_MULTICAST_ADDRESS = "226.1.1.3";

  /**
   * The address to use for binding the local socket for receiving multicast. Defaults to
   * 0.0.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-9961">HBASE-9961</a>
   */
  public static final String STATUS_MULTICAST_BIND_ADDRESS =
      "hbase.status.multicast.bind.address.ip";
  public static final String DEFAULT_STATUS_MULTICAST_BIND_ADDRESS = "0.0.0.0";

  /**
   * The port to use for the multicast messages.
   */
  public static final String STATUS_MULTICAST_PORT = "hbase.status.multicast.address.port";
  public static final int DEFAULT_STATUS_MULTICAST_PORT = 16100;

  /**
   * The network interface name to use for the multicast messages.
   */
  public static final String STATUS_MULTICAST_NI_NAME = "hbase.status.multicast.ni.name";

  /**
   * The address to use for binding the local socket for sending multicast. Defaults to 0.0.0.0.
   */
  public static final String STATUS_MULTICAST_PUBLISHER_BIND_ADDRESS =
      "hbase.status.multicast.publisher.bind.address.ip";
  public static final String DEFAULT_STATUS_MULTICAST_PUBLISHER_BIND_ADDRESS = "0.0.0.0";

  public static final long NO_NONCE = 0;

  /** Default cipher for encryption */
  public static final String CIPHER_AES = "AES";

  /** Configuration key for the crypto algorithm provider, a class name */
  public static final String CRYPTO_CIPHERPROVIDER_CONF_KEY = "hbase.crypto.cipherprovider";

  /** Configuration key for the crypto key provider, a class name */
  public static final String CRYPTO_KEYPROVIDER_CONF_KEY = "hbase.crypto.keyprovider";

  /** Configuration key for the crypto key provider parameters */
  public static final String CRYPTO_KEYPROVIDER_PARAMETERS_KEY =
      "hbase.crypto.keyprovider.parameters";

  /** Configuration key for the name of the master key for the cluster, a string */
  public static final String CRYPTO_MASTERKEY_NAME_CONF_KEY = "hbase.crypto.master.key.name";

  /** Configuration key for the name of the alternate master key for the cluster, a string */
  public static final String CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY =
      "hbase.crypto.master.alternate.key.name";

  /** Configuration key for the algorithm to use when encrypting the WAL, a string */
  public static final String CRYPTO_WAL_ALGORITHM_CONF_KEY = "hbase.crypto.wal.algorithm";

  /** Configuration key for the name of the master WAL encryption key for the cluster, a string */
  public static final String CRYPTO_WAL_KEY_NAME_CONF_KEY = "hbase.crypto.wal.key.name";

  /** Configuration key for the algorithm used for creating jks key, a string */
  public static final String CRYPTO_KEY_ALGORITHM_CONF_KEY = "hbase.crypto.key.algorithm";

  /** Configuration key for the name of the alternate cipher algorithm for the cluster, a string */
  public static final String CRYPTO_ALTERNATE_KEY_ALGORITHM_CONF_KEY =
      "hbase.crypto.alternate.key.algorithm";

  /** Configuration key for enabling WAL encryption, a boolean */
  public static final String ENABLE_WAL_ENCRYPTION = "hbase.regionserver.wal.encryption";

  /** Configuration key for setting RPC codec class name */
  public static final String RPC_CODEC_CONF_KEY = "hbase.client.rpc.codec";

  /** Configuration key for setting replication codec class name */
  public static final String REPLICATION_CODEC_CONF_KEY = "hbase.replication.rpc.codec";

  /** Maximum number of threads used by the replication source for shipping edits to the sinks */
  public static final String REPLICATION_SOURCE_MAXTHREADS_KEY =
      "hbase.replication.source.maxthreads";

  /**
   * Drop edits for tables that been deleted from the replication source and target
   * @deprecated moved it into HBaseInterClusterReplicationEndpoint
   */
  @Deprecated
  public static final String REPLICATION_DROP_ON_DELETED_TABLE_KEY =
      "hbase.replication.drop.on.deleted.table";

  /** Maximum number of threads used by the replication source for shipping edits to the sinks */
  public static final int REPLICATION_SOURCE_MAXTHREADS_DEFAULT = 10;

  /** Configuration key for SplitLog manager timeout */
  public static final String HBASE_SPLITLOG_MANAGER_TIMEOUT = "hbase.splitlog.manager.timeout";

  /**
   * Configuration keys for Bucket cache
   */
  // TODO moving these bucket cache implementation specific configs to this level is violation of
  // encapsulation. But as these has to be referred from hbase-common and bucket cache
  // sits in hbase-server, there were no other go! Can we move the cache implementation to
  // hbase-common?

  /**
   * Current ioengine options in include: heap, offheap and file:PATH (where PATH is the path
   * to the file that will host the file-based cache.  See BucketCache#getIOEngineFromName() for
   * list of supported ioengine options.
   * <p>Set this option and a non-zero {@link #BUCKET_CACHE_SIZE_KEY} to enable bucket cache.
   */
  public static final String BUCKET_CACHE_IOENGINE_KEY = "hbase.bucketcache.ioengine";

  /**
   * When using bucket cache, this is a float that EITHER represents a percentage of total heap
   * memory size to give to the cache (if &lt; 1.0) OR, it is the capacity in
   * megabytes of the cache.
   */
  public static final String BUCKET_CACHE_SIZE_KEY = "hbase.bucketcache.size";

  /**
   * HConstants for fast fail on the client side follow
   */
  /**
   * Config for enabling/disabling the fast fail mode.
   * @deprecated since 2.3.0, and in 3.0.0 the actually implementation will be removed so config
   *             this value will have no effect. The constants itself will be removed in 4.0.0.
   */
  @Deprecated
  public static final String HBASE_CLIENT_FAST_FAIL_MODE_ENABLED =
      "hbase.client.fast.fail.mode.enabled";

  /**
   * @deprecated since 2.3.0, and in 3.0.0 the actually implementation will be removed so config
   *             this value will have no effect. The constants itself will be removed in 4.0.0.
   */
  @Deprecated
  public static final boolean HBASE_CLIENT_ENABLE_FAST_FAIL_MODE_DEFAULT = false;

  /**
   * @deprecated since 2.3.0, and in 3.0.0 the actually implementation will be removed so config
   *             this value will have no effect. The constants itself will be removed in 4.0.0.
   */
  @Deprecated
  public static final String HBASE_CLIENT_FAST_FAIL_THREASHOLD_MS =
      "hbase.client.fastfail.threshold";

  /**
   * @deprecated since 2.3.0, and in 3.0.0 the actually implementation will be removed so config
   *             this value will have no effect. The constants itself will be removed in 4.0.0.
   */
  @Deprecated
  public static final long HBASE_CLIENT_FAST_FAIL_THREASHOLD_MS_DEFAULT = 60000;

  /**
   * @deprecated since 2.3.0, and in 3.0.0 the actually implementation will be removed so config
   *             this value will have no effect. The constants itself will be removed in 4.0.0.
   */
  @Deprecated
  public static final String HBASE_CLIENT_FAILURE_MAP_CLEANUP_INTERVAL_MS =
      "hbase.client.failure.map.cleanup.interval";

  /**
   * @deprecated since 2.3.0, and in 3.0.0 the actually implementation will be removed so config
   *             this value will have no effect. The constants itself will be removed in 4.0.0.
   */
  @Deprecated
  public static final long HBASE_CLIENT_FAILURE_MAP_CLEANUP_INTERVAL_MS_DEFAULT = 600000;

  /**
   * @deprecated since 2.3.0, and in 3.0.0 the actually implementation will be removed so config
   *             this value will have no effect. The constants itself will be removed in 4.0.0.
   */
  @Deprecated
  public static final String HBASE_CLIENT_FAST_FAIL_CLEANUP_MS_DURATION_MS =
      "hbase.client.fast.fail.cleanup.duration";

  /**
   * @deprecated since 2.3.0, and in 3.0.0 the actually implementation will be removed so config
   *             this value will have no effect. The constants itself will be removed in 4.0.0.
   */
  @Deprecated
  public static final long HBASE_CLIENT_FAST_FAIL_CLEANUP_DURATION_MS_DEFAULT = 600000;

  /**
   * @deprecated since 2.3.0, and in 3.0.0 the actually implementation will be removed so config
   *             this value will have no effect. The constants itself will be removed in 4.0.0.
   */
  @Deprecated
  public static final String HBASE_CLIENT_FAST_FAIL_INTERCEPTOR_IMPL =
      "hbase.client.fast.fail.interceptor.impl";

  /**
   * @deprecated since 2.4.0 and in 3.0.0, to be removed in 4.0.0, replaced by procedure-based
   *   distributed WAL splitter; see SplitWALManager.
   */
  @Deprecated
  public static final String HBASE_SPLIT_WAL_COORDINATED_BY_ZK = "hbase.split.wal.zk.coordinated";

  /**
   * @deprecated since 2.4.0 and in 3.0.0, to be removed in 4.0.0.
   */
  @Deprecated
  public static final boolean DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK = false;

  public static final String HBASE_SPLIT_WAL_MAX_SPLITTER = "hbase.regionserver.wal.max.splitters";

  public static final int DEFAULT_HBASE_SPLIT_WAL_MAX_SPLITTER = 2;

  /** Config key for if the server should send backpressure and if the client should listen to
   * that backpressure from the server */
  public static final String ENABLE_CLIENT_BACKPRESSURE = "hbase.client.backpressure.enabled";
  public static final boolean DEFAULT_ENABLE_CLIENT_BACKPRESSURE = false;

  public static final String HEAP_OCCUPANCY_LOW_WATERMARK_KEY =
      "hbase.heap.occupancy.low_water_mark";
  public static final float DEFAULT_HEAP_OCCUPANCY_LOW_WATERMARK = 0.95f;
  public static final String HEAP_OCCUPANCY_HIGH_WATERMARK_KEY =
      "hbase.heap.occupancy.high_water_mark";
  public static final float DEFAULT_HEAP_OCCUPANCY_HIGH_WATERMARK = 0.98f;

  /**
   * The max number of threads used for splitting storefiles in parallel during
   * the region split process.
   */
  public static final String REGION_SPLIT_THREADS_MAX =
      "hbase.regionserver.region.split.threads.max";

  /** Canary config keys */
  // TODO: Move these defines to Canary Class
  public static final String HBASE_CANARY_WRITE_DATA_TTL_KEY = "hbase.canary.write.data.ttl";

  public static final String HBASE_CANARY_WRITE_PERSERVER_REGIONS_LOWERLIMIT_KEY =
      "hbase.canary.write.perserver.regions.lowerLimit";

  public static final String HBASE_CANARY_WRITE_PERSERVER_REGIONS_UPPERLIMIT_KEY =
      "hbase.canary.write.perserver.regions.upperLimit";

  public static final String HBASE_CANARY_WRITE_VALUE_SIZE_KEY = "hbase.canary.write.value.size";

  public static final String HBASE_CANARY_WRITE_TABLE_CHECK_PERIOD_KEY =
      "hbase.canary.write.table.check.period";

  public static final String HBASE_CANARY_READ_RAW_SCAN_KEY = "hbase.canary.read.raw.enabled";

  public static final String HBASE_CANARY_READ_ALL_CF = "hbase.canary.read.all.column.famliy";
  /**
   * Configuration keys for programmatic JAAS configuration for secured ZK interaction
   */
  public static final String ZK_CLIENT_KEYTAB_FILE = "hbase.zookeeper.client.keytab.file";
  public static final String ZK_CLIENT_KERBEROS_PRINCIPAL =
      "hbase.zookeeper.client.kerberos.principal";
  public static final String ZK_SERVER_KEYTAB_FILE = "hbase.zookeeper.server.keytab.file";
  public static final String ZK_SERVER_KERBEROS_PRINCIPAL =
      "hbase.zookeeper.server.kerberos.principal";

  /** Config key for hbase temporary directory in hdfs */
  public static final String TEMPORARY_FS_DIRECTORY_KEY = "hbase.fs.tmp.dir";
  public static final String DEFAULT_TEMPORARY_HDFS_DIRECTORY = "/user/"
      + System.getProperty("user.name") + "/hbase-staging";

  public static final String SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT =
      "hbase.snapshot.restore.take.failsafe.snapshot";
  public static final boolean DEFAULT_SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT = true;

  public static final String SNAPSHOT_RESTORE_FAILSAFE_NAME =
      "hbase.snapshot.restore.failsafe.name";
  public static final String DEFAULT_SNAPSHOT_RESTORE_FAILSAFE_NAME =
      "hbase-failsafe-{snapshot.name}-{restore.timestamp}";

  public static final String DEFAULT_LOSSY_COUNTING_ERROR_RATE =
      "hbase.util.default.lossycounting.errorrate";
  public static final String NOT_IMPLEMENTED = "Not implemented";

  // Default TTL - FOREVER
  public static final long DEFAULT_SNAPSHOT_TTL = 0;

  // User defined Default TTL config key
  public static final String DEFAULT_SNAPSHOT_TTL_CONFIG_KEY = "hbase.master.snapshot.ttl";

  // Regions Recovery based on high storeFileRefCount threshold value
  public static final String STORE_FILE_REF_COUNT_THRESHOLD =
      "hbase.regions.recovery.store.file.ref.count";

  // default -1 indicates there is no threshold on high storeRefCount
  public static final int DEFAULT_STORE_FILE_REF_COUNT_THRESHOLD = -1;

  public static final String REGIONS_RECOVERY_INTERVAL =
      "hbase.master.regions.recovery.check.interval";

  public static final int DEFAULT_REGIONS_RECOVERY_INTERVAL = 1200 * 1000; // Default 20 min

  /**
   * Configurations for master executor services.
   */
  public static final String MASTER_OPEN_REGION_THREADS =
      "hbase.master.executor.openregion.threads";
  public static final int MASTER_OPEN_REGION_THREADS_DEFAULT = 5;

  public static final String MASTER_CLOSE_REGION_THREADS =
      "hbase.master.executor.closeregion.threads";
  public static final int MASTER_CLOSE_REGION_THREADS_DEFAULT = 5;

  public static final String MASTER_SERVER_OPERATIONS_THREADS =
      "hbase.master.executor.serverops.threads";
  public static final int MASTER_SERVER_OPERATIONS_THREADS_DEFAULT = 5;

  /**
   * Number of threads used to dispatch merge operations to the regionservers.
   */
  public static final String MASTER_MERGE_DISPATCH_THREADS =
      "hbase.master.executor.merge.dispatch.threads";
  public static final int MASTER_MERGE_DISPATCH_THREADS_DEFAULT = 2;

  public static final String MASTER_META_SERVER_OPERATIONS_THREADS =
      "hbase.master.executor.meta.serverops.threads";
  public static final int MASTER_META_SERVER_OPERATIONS_THREADS_DEFAULT = 5;

  public static final String MASTER_LOG_REPLAY_OPS_THREADS =
      "hbase.master.executor.logreplayops.threads";
  public static final int MASTER_LOG_REPLAY_OPS_THREADS_DEFAULT = 10;

  public static final int DEFAULT_SLOW_LOG_RING_BUFFER_SIZE = 256;

  public static final String SLOW_LOG_BUFFER_ENABLED_KEY =
      "hbase.regionserver.slowlog.buffer.enabled";
  public static final boolean DEFAULT_ONLINE_LOG_PROVIDER_ENABLED = false;

  /** The slowlog info family as a string*/
  private static final String SLOWLOG_INFO_FAMILY_STR = "info";

  /** The slowlog info family */
  public static final byte [] SLOWLOG_INFO_FAMILY = Bytes.toBytes(SLOWLOG_INFO_FAMILY_STR);

  public static final String SLOW_LOG_SYS_TABLE_ENABLED_KEY =
      "hbase.regionserver.slowlog.systable.enabled";
  public static final boolean DEFAULT_SLOW_LOG_SYS_TABLE_ENABLED_KEY = false;

  public static final String SHELL_TIMESTAMP_FORMAT_EPOCH_KEY =
      "hbase.shell.timestamp.format.epoch";

  public static final boolean DEFAULT_SHELL_TIMESTAMP_FORMAT_EPOCH = false;

  /**
   * Number of rows in a batch operation above which a warning will be logged.
   */
  public static final String BATCH_ROWS_THRESHOLD_NAME = "hbase.rpc.rows.warning.threshold";

  /**
   * Default value of {@link #BATCH_ROWS_THRESHOLD_NAME}
   */
  public static final int BATCH_ROWS_THRESHOLD_DEFAULT = 5000;

  private HConstants() {
    // Can't be instantiated with this ctor.
  }
}
