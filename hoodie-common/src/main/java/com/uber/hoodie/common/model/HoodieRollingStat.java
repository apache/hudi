package com.uber.hoodie.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import javax.annotation.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieRollingStat implements Serializable {

  private String fileId;
  private long inserts;
  private long upserts;
  // TODO
  @Nullable
  private long totalInputWriteBytesToDisk;
  @Nullable
  private long totalInputWriteBytesOnDisk;

  public HoodieRollingStat() {
    // called by jackson json lib
  }

  public HoodieRollingStat(String fileId, long inserts, long upserts, long totalInputWriteBytesOnDisk) {
    this.fileId = fileId;
    this.inserts = inserts;
    this.upserts = upserts;
    this.totalInputWriteBytesOnDisk = totalInputWriteBytesOnDisk;
  }

  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public long getInserts() {
    return inserts;
  }

  public void setInserts(long inserts) {
    this.inserts = inserts;
  }

  public long getUpserts() {
    return upserts;
  }

  public void setUpserts(long upserts) {
    this.upserts = upserts;
  }
  public long updateInserts(long inserts) {
    this.inserts += inserts;
    return this.inserts;
  }

  public long updateUpserts(long upserts) {
    this.upserts += upserts;
    return this.upserts;
  }

  public long getTotalInputWriteBytesOnDisk() {
    return totalInputWriteBytesOnDisk;
  }
}
