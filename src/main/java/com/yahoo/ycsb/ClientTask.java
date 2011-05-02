package com.yahoo.ycsb;

import java.util.Properties;
import java.util.concurrent.Callable;

/**
* Created by IntelliJ IDEA. User: tdunning Date: 2/14/11 Time: 3:15 PM To change this template use File | Settings |
* File Templates.
*/
class ClientTask implements Callable<Integer> {
  private volatile int opsDone = 0;

  private DB db;
  private Workload workload;

  private Properties properties;
  private Object workloadState;

  // for testing
  protected ClientTask() {
  }

  public ClientTask(DB db, Workload workload, int threadId, int threadCount, Properties props) throws DBException, WorkloadException {
    this.db = db;
    this.workload = workload;
    this.properties = props;

    db.init();
    workloadState = workload.initThread(properties, threadId, threadCount);
  }


  public int getOpsDone() {
    return opsDone;
  }

  @Override
  public Integer call() throws DBException {
    if (!workload.doTransaction(db, workloadState)) {
      throw new DBException("Workload says we are done");
    }
    opCompleted();
    return opsDone;
  }

  public void opCompleted() {
    opsDone++;
  }

  public int finish() throws DBException {
    db.cleanup();
    return opsDone;
  }
}
