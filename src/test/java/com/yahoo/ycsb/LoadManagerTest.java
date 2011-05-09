package com.yahoo.ycsb;

import com.google.common.io.CharStreams;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Properties;

public class LoadManagerTest {
  private static final Logger log = Logger.getLogger(LoadManagerTest.class);

  private static File tmpdir;
  private static ZooKeeperServer zs;
  private static int zkPort = 5555;
  private static ZooKeeper zk;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException, KeeperException {
    tmpdir = File.createTempFile("zkTestData", "");
    tmpdir.delete();
    tmpdir.mkdir();

    zs = new ZooKeeperServer(tmpdir, tmpdir, 1000);
    SyncRequestProcessor.setSnapCount(150);
    NIOServerCnxn.Factory f = new NIOServerCnxn.Factory(new InetSocketAddress(zkPort));
    f.startup(zs);
    log.info("starting up the zookeeper server .. waiting");
    Assert.assertTrue("waiting for server being up", waitForServerUp(zkPort, 2000));

    zk = new ZooKeeper("localhost:" + zkPort, 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // do nothing
      }
    });

    zk.create("/ycsb", "foo".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  private static boolean waitForServerUp(int port, int timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        // if there are multiple hostports, just take the first one
        String result = send4LetterWord("localhost", port, "stat");
        if (result.startsWith("Zookeeper version:")) {
          return true;
        }
      } catch (IOException e) {
        // ignore as this is expected
        log.info("server " + " not up " + e);
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;

  }

  private static String send4LetterWord(String host, int port, String command) throws IOException {
    Socket sock = new Socket(host, port);

    OutputStream outstream = sock.getOutputStream();
    outstream.write(command.getBytes());
    outstream.flush();

    String r = CharStreams.readLines(new InputStreamReader(sock.getInputStream())).get(0);
    sock.close();

    return r;
  }

  @Test
  public void noSuchFileAtFirst() throws IOException, InterruptedException, KeeperException {
    final boolean[] state = new boolean[2];
    LoadManager lg = new LoadManager("/ycsb/x", "localhost:" + zkPort, new LoadManager.LoadGenerator() {
      public boolean stopped = false;

      @Override
      public void load(Properties properties) {
        state[0] = true;
        Assert.assertTrue(stopped);
        Assert.assertEquals("bar", properties.get("foo"));
      }

      @Override
      public void stop() {
        state[1] = true;
        stopped = true;
      }
    });
    Thread.sleep(100);

    // with no file initially, the load generator gets a stop command
    Assert.assertFalse(state[0]);
    Assert.assertTrue(state[1]);

    // then we create the file
    zk.create("/ycsb/x", "foo: bar\n".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    Thread.sleep(10);

    // the listener should see it appear pretty much instantly
    Assert.assertTrue(state[0]);
    Assert.assertTrue(state[1]);

    // now let's watch the deletion behavior
    state[0] = false;
    state[1] = false;

    zk.delete("/ycsb/x", -1);
    Thread.sleep(10);

    // should have done a stop
    Assert.assertFalse(state[0]);
    Assert.assertTrue(state[1]);
  }

  @Test
  public void sessionExpiration() throws InterruptedException, KeeperException, IOException {

    final boolean[] state = new boolean[2];
    zk.create("/ycsb/x", "foo: bar\n".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    LoadManager lm = new LoadManager("/ycsb/x", "localhost:" + zkPort, new LoadManager.LoadGenerator() {
      public boolean stopped = false;

      @Override
      public void load(Properties properties) {
        state[0] = true;
        Assert.assertTrue(stopped);
        Assert.assertEquals("bar", properties.get("foo"));
      }

      @Override
      public void stop() {
        state[1] = true;
        stopped = true;
      }
    });
    Thread.sleep(100);

    // the listener should see it appear pretty much instantly
    Assert.assertTrue(state[0]);
    Assert.assertFalse(state[1]);

    // now we clone the zk connection from the LoadManager and we cause session expiration
    lm.getZk().close();

    Thread.sleep(5000);
    Assert.assertTrue(state[1]);
  }

  @AfterClass
  public static void teardown() throws InterruptedException, IOException {
    zk.close();
    zs.shutdown();
    delete(tmpdir);
  }

  public static void delete(File f) {
    if (f.isDirectory()) {
      for (File file : f.listFiles()) {
        delete(file);
      }
      f.delete();
    } else {
      f.delete();
    }
  }
}
