package com.uber.hoodie;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class KeyTranslator implements Serializable {

  DateFormat simple = new SimpleDateFormat("yyyy-MM-dd");

  public String translateRecordKey(String recordKey) {
    return recordKey;
  }

  public String translatePartitionPath(String partitionPath) {
    Date date = new Date();
    date.setTime(Long.valueOf(partitionPath));
    return simple.format(partitionPath).replace("-", "/");
  }
}
