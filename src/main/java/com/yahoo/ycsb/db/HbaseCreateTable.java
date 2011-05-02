package com.yahoo.ycsb.db;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class HbaseCreateTable {
  public static void main(String[] args) throws IOException, MasterNotRunningException {
    int regions = 50;
    if (args.length > 0) {
      regions = Integer.parseInt(args[0]);
    }
    final Configuration configuration = new Configuration();
    configuration.addResource("hbase-site.xml");
    configuration.set("clientPort", "5181");
    System.out.printf("%s\n", configuration.toString());
    HBaseAdmin admin = new HBaseAdmin(configuration);
    final HTableDescriptor descriptor = new HTableDescriptor("usertable");
    descriptor.addFamily(new HColumnDescriptor("family"));

    long start = 0;
    long end = (1L << 31);
    long delta = (end - start) / (regions + 1);
    List<String> splitKeys = Lists.newArrayList();
    long split = start;
    for (int i = 0; i < regions; i++) {
      splitKeys.add("user" + split);
      split += delta;
    }
    Collections.sort(splitKeys);
    System.out.printf("%s\n", splitKeys);

    byte[][] splits = new byte[regions][];
    int i = 0;
    for (String key : splitKeys) {
      splits[i++] = key.getBytes();
    }
    admin.createTable(descriptor, splits);
  }
}
