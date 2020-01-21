package org.apache.hudi.exception;

public class HoodieParseException extends HoodieException {
    public HoodieParseException(String msg, Throwable e) {
        super(msg, e);
    }
}
