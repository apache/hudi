package com.uber.hoodie.utilities.exception;

import com.uber.hoodie.exception.HoodieException;

public class HoodieWorkloadManagerException extends HoodieException {

  public HoodieWorkloadManagerException(String msg, Throwable e) {
    super(msg, e);
  }

  public HoodieWorkloadManagerException(String msg) {
    super(msg);
  }

}
