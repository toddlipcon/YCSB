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
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manages load generation and reconfiguration of same.  The load to be generated is specified by a
 * properties file that is either found in ZK or found in the local file system. If the file is in
 * ZK, then every time the file changes, it is reloaded and the load being generated is changed to
 * conform to the new settings.
 */
public class LoadManager {
  private static final int SESSION_TIMEOUT = 20000;
  private final Logger log = Logger.getLogger(LoadManager.class);
  private final String zookeeperServers;
  private final Watcher watcher = new Watcher() {
    @Override
    public void process(WatchedEvent watchedEvent) {
      log.info("Saw " + watchedEvent.getType() + " on " + watchedEvent.getPath());
      switch (watchedEvent.getType()) {
        case NodeDataChanged:
        case NodeCreated:
          // load data
          if (propDirectory.equals(watchedEvent.getPath())) {
            reloadFromZookeeper();
          }
          break;

        case NodeDeleted:
          if (propFile.equals(watchedEvent.getPath())) {
            lg.stop();
          }

        default:
          // event type None probably means we temporarily lost our ZK connection
          // nothing to do for that but keep on keeping on
        case None:

          // it makes no sense to put children under a workload file
        case NodeChildrenChanged:
          if (propDirectory.equals(watchedEvent.getPath())) {
            reloadFromZookeeper();
          }
          break;
      }
    }
  };

  private String propDirectory;
  private String propFile;
  private ZooKeeper zk;
  private int propFileVersion = -1;

  private LoadGenerator lg;
  private ScheduledExecutorService restartService = Executors.newScheduledThreadPool(1);
  private ScheduledFuture<?> checker = null;

  public LoadManager(String propFile, String zkServers, LoadGenerator lg) throws IOException, InterruptedException, KeeperException {
    this.propFile = propFile;
    this.propDirectory = propFile.replaceAll("/[^/]+$", "");
    this.zookeeperServers = zkServers;
    this.lg = lg;
    if (zkServers != null) {
      // set up monitoring
      init(new ZooKeeper(zookeeperServers, SESSION_TIMEOUT, watcher));
    } else {
      // load once only
      reload(new FileInputStream(propFile));
    }
  }

  private synchronized void init(ZooKeeper zk) {
    this.zk = zk;

    // check on the workload version in ZK every few seconds to make sure that we never lose
    // track even if we had a session expiration or something.
    if (checker != null) {
      // cancel, but don't interrupt
      checker.cancel(false);
    }
    checker = restartService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        // check for new version and make sure that we get notified for any changes.
        LoadManager.this.reloadFromZookeeper();
      }
    }, 0, 2, TimeUnit.SECONDS);
  }

  /**
   * Gets the configuration from ZK and resets the watch on the config.  If the version of the data
   * is newer than what we are running, then we reload.
   */
  private synchronized void reloadFromZookeeper()  {
    try {
      if (zk == null) {
        log.info("Reopening ZK connection");
        init(new ZooKeeper(zookeeperServers, SESSION_TIMEOUT, watcher));
      }
    } catch (IOException e) {
      log.error("Can't re-open zookeeper connection");
      // stop serving
      return;
    }

    if (zk != null) {
        // get children to ensure watch is set on parent dir
      try {
        zk.getChildren(propDirectory, true);
      } catch (KeeperException.SessionExpiredException e) {
        // reset zk connection so it will get re-opened later
        zk = null;
        log.warn("Session expired");
        return;
      } catch (KeeperException e) {
        // ignore
      } catch (InterruptedException e) {
        // ignore
      }

      try {
        Stat stat = new Stat();

        // but really we want the data
        byte[] data = this.zk.getData(propFile, true, stat);
        log.debug("Got [" + new String(data) + "]");
        if (stat.getVersion() != propFileVersion) {
          log.info("New version: " + stat.getVersion());
          try {
            this.reload(new ByteArrayInputStream(data));
          } catch (IOException e) {
            // can't happen (really!)
            log.error("I/O error reading properties from bytes", e);
          }
          propFileVersion = stat.getVersion();
        } else {
          log.info("Old version:" + stat.getVersion());
        }
      } catch (KeeperException.SessionExpiredException e) {
        // reset zk connection so it will get re-opened later
        zk = null;
        log.warn("Session expired");
      } catch (KeeperException.NoNodeException e) {
        // config file missing ... nothing to do
        lg.stop();
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

  public ZooKeeper getZk() {
    return zk;
  }

  /**
   * Restarts the load from the specification given
   *
   * @param input Where to get the workload spec from.
   */
  private void reload(InputStream input) throws IOException {
    Properties p = new Properties();
    p.load(input);
    lg.load(p);
  }


  interface LoadGenerator {
    void load(Properties properties);

    void stop();
  }
}
