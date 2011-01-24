package com.yahoo.ycsb.workloads;

/**
 * Created by IntelliJ IDEA. User: tdunning Date: 1/23/11 Time: 5:25 PM To change this template use
 * File | Settings | File Templates.
 */
public class DataCorruptionException extends RuntimeException {
  public DataCorruptionException(String msg) {
    super(msg);
  }
}
