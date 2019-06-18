/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.timeline;

import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Represents the Archived Timeline for the HoodieDataset. Instants for the last 12 hours
 * (configurable) is in the ActiveTimeline and the rest are in ArchivedTimeline. <p></p> Instants
 * are read from the archive file during initialization and never refreshed. To refresh, clients
 * need to call reload() <p></p> This class can be serialized and de-serialized and on
 * de-serialization the FileSystem is re-initialized.
 */
public class HoodieArchivedTimeline extends HoodieDefaultTimeline {

  private static final String HOODIE_COMMIT_ARCHIVE_LOG_FILE = "commits";
  private HoodieTableMetaClient metaClient;
  private Map<String, byte[]> readCommits = new HashMap<>();

  private static final transient Logger log = LogManager.getLogger(HoodieArchivedTimeline.class);

  public HoodieArchivedTimeline(HoodieTableMetaClient metaClient) {
    // Read back the commits to make sure
    Path archiveLogPath = HoodieArchivedTimeline.getArchiveLogPath(metaClient.getArchivePath());
    try (SequenceFile.Reader reader =
        new SequenceFile.Reader(metaClient.getHadoopConf(),
            SequenceFile.Reader.file(archiveLogPath))) {
      Text key = new Text();
      Text val = new Text();
      while (reader.next(key, val)) {
        // TODO - limit the number of commits loaded in memory. this could get very large.
        // This is okay because only tooling will load the archived commit timeline today
        readCommits.put(key.toString(), Arrays.copyOf(val.getBytes(), val.getLength()));
      }
      this.setInstants(readCommits.keySet().stream().map(
          s -> new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, s)).collect(
          Collectors.toList()));
    } catch (IOException e) {
      throw new HoodieIOException(
          "Could not load archived commit timeline from path " + archiveLogPath, e);
    }
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.details =
        (Function<HoodieInstant, Optional<byte[]>> & Serializable) this::getInstantDetails;
    this.metaClient = metaClient;
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  public HoodieArchivedTimeline() {
  }

  /**
   * This method is only used when this object is deserialized in a spark executor.
   *
   * @deprecated
   */
  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }


  public static Path getArchiveLogPath(String archiveFolder) {
    return new Path(archiveFolder, HOODIE_COMMIT_ARCHIVE_LOG_FILE);
  }

  @Override
  public Optional<byte[]> getInstantDetails(HoodieInstant instant) {
    return Optional.ofNullable(readCommits.get(instant.getTimestamp()));
  }

  public HoodieArchivedTimeline reload() {
    return new HoodieArchivedTimeline(metaClient);
  }

}
