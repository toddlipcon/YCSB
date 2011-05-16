package com.yahoo.ycsb.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperClient {
  ZooKeeper zk;
  String zkConnectString;
  private final int TIMEOUT = 30 * 1000;
  private Wrapper obj = null;
  
  public ZooKeeperClient(String zkConnectString, String zkTriggerPath) throws Exception {
    try {
      obj = new Wrapper();
      YCSBWatcher watcher = new YCSBWatcher(zkTriggerPath, obj);
      this.zk = new ZooKeeper(zkConnectString, TIMEOUT, watcher);
      // Go to sleep
      try {
        System.out.println("Waiting for ZK Trigger file to get created..." +
            " file: " + zkTriggerPath);
        Stat stat = zk.exists(zkTriggerPath, watcher);
        if (stat != null) {
          System.out.println("zkTrigger path already exists... Not a clean start. Terminating");
          System.exit(1);
        }
        obj.w();
      } catch (InterruptedException e) {
        // Good to start
        System.out.println("Node created...");
      }
    } catch (Exception e) {
      System.out.println("ZooKeeperClient Init : " + e);
      throw e;
    }
  }
  
  class Wrapper {
    public synchronized void w() throws InterruptedException {
      wait();
    }
    
    public synchronized void n() {
      notify();
    }
  }

  public ZooKeeper getZooKeeper() {
    return this.zk;
  }

  class YCSBWatcher implements Watcher {
    String zkTriggerPath = null;
    Wrapper w = null;
    
    public YCSBWatcher(String zkTriggerPath, Wrapper w) {
      this.zkTriggerPath = zkTriggerPath;
      this.w = w;
    }
    
    
    @Override
    public void process(WatchedEvent event) {
      String path = event.getPath();
      System.out.println("ZooKeeper event " + event.getType() + " on path " +
          event.getPath());
    
      if ( event.getState() == KeeperState.SyncConnected ) {
    	  // Connected
    	}
      if (event.getType() == Event.EventType.None) {
        // Connection state changed
        switch (event.getState()) {
          case SyncConnected:
            // read ActiveZNodes and make sure we are still active
            break;
          case Expired:
            // Lost connection to ZK
            
            System.out.println("Lost ZooKeeper connection. Termination program...");
            System.exit(1);
            break;
        } 
      } else {
        if (path != null &&
            path.equals(this.zkTriggerPath)) {
          // Something has changed on the node, let's find out
          switch (event.getType()) {
          case NodeCreated:
            System.out.println("ZooKeeper event of type: " + event.getType() +
                " on path " + event.getPath());
            w.n();
            break;
          default:
            System.out.println("ZooKeeper event of type: " + event.getType() +
                " on path " + event.getPath());
            break;
          }
        }
      }
    }
  }
}
