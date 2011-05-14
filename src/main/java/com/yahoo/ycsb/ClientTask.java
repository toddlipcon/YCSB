package com.yahoo.ycsb;

import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Describes the overall structure of a workload task.  This is where we delegate operations to the
 * actual workload object.
 */
class ClientTask implements Callable<Integer> {
  private volatile int opsDone = 0;

  private DB db;
  private Workload workload;
  private boolean doTransactions;

  private Properties properties;
  private Object workloadState;

  // for testing
  protected ClientTask() {
  }

  public ClientTask(DB db, Workload workload, int threadId, int threadCount,
      Properties props, boolean doTransactions) throws DBException, WorkloadException {
    this.db = db;
    this.workload = workload;
    this.properties = props;
    this.doTransactions = doTransactions;

    db.init();
    workloadState = workload.initThread(properties, threadId, threadCount);
  }


  public int getOpsDone() {
    return opsDone;
  }

  @Override
  public Integer call() throws DBException {
    boolean success = false;
    
    if (!doTransactions) {
      success = workload.doInsert(db, workloadState);
    } else {
      success = workload.doTransaction(db, workloadState);
    }
    if (!success) {
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
