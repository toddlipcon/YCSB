package com.yahoo.ycsb.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

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

    String startKey = "user1020000000";
    String endKey = "user940000000";
    admin.createTable(descriptor, startKey.getBytes(), endKey.getBytes(), regions);
  }
}
