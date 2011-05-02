package com.yahoo.ycsb;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages load generation and reconfiguration of same.  The load to be generated is specified by a
 * properties file that is either found in ZK or found in the local file system. If the file is in
 * ZK, then every time the file changes, it is reloaded and the load being generated is changed to
 * conform to the new settings.
 */
public class LoadGenerator {
  private final Logger log = Logger.getLogger(LoadGenerator.class);
  private final String zookeeperServers;

  private String propFile;
  private ZooKeeper zk;
  private int propFileVersion = -1;

  public LoadGenerator(String propFile, String zk) throws IOException, InterruptedException, KeeperException {
    this.propFile = propFile;
    this.zookeeperServers = zk;
    if (zk != null) {
      // set up monitoring
      init(new ZooKeeper(zookeeperServers, 20, null));
    } else {
      // load once only
      reload(new FileInputStream(propFile));
    }
  }

  public LoadGenerator(String propFile, ZooKeeper zk) {
    this.propFile = propFile;
    zookeeperServers = null;
    init(zk);
  }

  private void init(ZooKeeper zk) {
    zk.register(new Watcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
          case NodeDataChanged:
          case NodeCreated:
            if (watchedEvent.getPath().equals(propFile)) {
              // load data
              reloadFromZookeeper();
            }
            break;
          case NodeDeleted:
            // stop load

          default:
            // event type None probably means we temporarily lost our ZK connection
            // nothing to do for that but keep on keeping on
          case None:

            // it makes no sense to put children under a workload file
          case NodeChildrenChanged:
            log.warn("Unexpected addition of children under property file (may have lost watch)");
            reloadFromZookeeper();
            break;
        }
      }
    });

    // check on the workload version in ZK every few seconds to make sure that we never lose
    // track even if we had a session expiration or something.
    ScheduledExecutorService checker = Executors.newScheduledThreadPool(1);
    checker.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        // check for new version and make sure that we get notified for any changes.
        LoadGenerator.this.reloadFromZookeeper();
      }
    }, 10, 30, TimeUnit.SECONDS);
  }

  /**
   * Gets the configuration from ZK and resets the watch on the config.  If the version of the data
   * is newer than what we are running, then we reload.
   */
  private synchronized void reloadFromZookeeper() {
    try {
      if (zk == null) {
        init(new ZooKeeper(zookeeperServers, 20, null));
      }
    } catch (IOException e) {
      log.error("Can't re-open zookeeper connection");
      // stop serving
      return;
    }

    if (zk != null) {
      try {
        Stat stat = new Stat();
        byte[] data = this.zk.getData(propFile, true, stat);
        if (stat.getVersion() != propFileVersion) {
          this.reload(new ByteArrayInputStream(data));
          propFileVersion = stat.getVersion();
        }
      } catch (KeeperException.SessionExpiredException e) {
        // reset zk connection so it will get re-opened later
        zk = null;
      } catch (KeeperException e) {
        log.error("Error getting workload file from ZK", e);
        // don't bother trying to read again right now... the file will be checked again shortly
      } catch (InterruptedException e) {
        log.error("Interruption while getting workload file from ZK", e);
        // ignore this.  This can't happen as far as we expect and the file
        // will be checked again shortly
      }
    }
  }

  /**
   * Restarts the load from the specification given
   *
   * @param inputStream Where to get the workload spec from.
   */
  private void reload(InputStream inputStream) {

  }
}
