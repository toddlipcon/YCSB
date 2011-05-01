/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb.db;


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.ycsb.DBException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * HBase client for YCSB framework
 */
public class HBaseClient extends com.yahoo.ycsb.DB {
  public boolean debug = false;

  public String table = "";
  public HTable hTable = null;
  public String columnFamily = "";
  public byte columnFamilyBytes[];

  public static final int Ok = 0;
  public static final int ServerError = -1;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  public void init() throws DBException {
    if ((getProperties().getProperty("debug") != null) &&
      (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }

    columnFamily = getProperties().getProperty("columnfamily");
    if (columnFamily == null) {
      System.err.println("Error, must specify a columnfamily for HBase table");
      throw new DBException("No columnfamily specified");
    }
    columnFamilyBytes = Bytes.toBytes(columnFamily);
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB instance per client
   * thread.
   */
  public void cleanup() throws DBException {
    try {
      if (hTable != null) {
        hTable.flushCommits();
      }
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  public void getHTable(String table) throws IOException {
    synchronized (this) {
      Configuration config = HBaseConfiguration.create();
      //HBaseAdmin.checkHBaseAvailable(config);

      HBaseAdmin hbAdmin = new HBaseAdmin(config);
      if (!hbAdmin.tableExists(table)) {
        HTableDescriptor desc = new HTableDescriptor(table);
        //   			for ( String cfamilyName : cfamilyNames ) {
        HColumnDescriptor cdesc = new HColumnDescriptor(columnFamily);
        desc.addFamily(cdesc);
        //   			}
        hbAdmin.createTable(desc);
      }

      if (hTable == null) {
        hTable = new HTable(config, table);
        //2 suggestions from http://ryantwopointoh.blogspot.com/2009/01/performance-of-hbase-importing.html
        hTable.setAutoFlush(false);
        hTable.setWriteBufferSize(1024 * 1024 * 12);
        //return hTable;
      }
    }

  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a
   * HashMap.
   *
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public int read(String table, String key, Iterable<String> fields, Map<String, String> result) {
    //if this is a "new" table, init HTable object.  Else, use existing one
    if (!this.table.equals(table)) {
      hTable = null;
      try {
        getHTable(table);
        this.table = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return ServerError;
      }
    }

    Result r;
    try {
      if (debug) {
        System.out.println("Doing read from HBase columnfamily " + columnFamily);
        System.out.println("Doing read for key: " + key);
      }
      Get g = new Get(Bytes.toBytes(key));
      r = hTable.get(g);
    } catch (IOException e) {
      System.err.println("Error doing get: " + e);
      return ServerError;
    } catch (ConcurrentModificationException e) {
      //do nothing for now...need to understand HBase concurrency model better
      return ServerError;
    }

    //now parse out all desired fields
    if (fields != null) {
      for (String field : fields) {
        byte[] value = r.getValue(columnFamilyBytes, Bytes.toBytes(field));
        result.put(field, Bytes.toString(value));
        if (debug) {
          System.out.println("Result for field: " + field + " is: " + Bytes.toString(value));
        }
      }
    }
    return Ok;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the
   * result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one
   *                    record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public int scan(String table, String startkey, int recordcount, Iterable<String> fields, List<Map<String, String>> result) {
  //if this is a "new" table, init HTable object.  Else, use existing one
  if (!this.table.equals(table)) {
    hTable = null;
    try {
      getHTable(table);
      this.table = table;
    } catch (IOException e) {
      System.err.println("Error accessing HBase table: " + e);
      return ServerError;
    }
  }

  Scan s = new Scan(Bytes.toBytes(startkey));
  //HBase has no record limit.  Here, assume recordcount is small enough to bring back in one call.
  //We get back recordcount records
  s.setCaching(recordcount);

  //add specified fields or else all fields
  if (fields == null) {
    s.addFamily(columnFamilyBytes);
  } else {
    for (String field : fields) {
      s.addColumn(columnFamilyBytes, Bytes.toBytes(field));
    }
  }

  //get results
  ResultScanner scanner = null;
  try {
    scanner = hTable.getScanner(s);
    int numResults = 0;
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      //get row key
      String key = Bytes.toString(rr.getRow());
      if (debug) {
        System.out.println("Got scan result for key: " + key);
      }

      Map<String, String> rowResult = new HashMap<String, String>();

      //parse row
      if (fields != null) {
        //parse specified field list
        for (String field : fields) {
          byte[] value = rr.getValue(columnFamilyBytes, Bytes.toBytes(field));
          rowResult.put(field, Bytes.toString(value));
          if (debug) {
            System.out.println("Result for field: " + field + " is: " + Bytes.toString(value));
          }
        }
      } else {
        //get all fields
        //HBase can return a mapping for all columns in a column family
        NavigableMap<byte[], byte[]> scanMap = rr.getFamilyMap(columnFamilyBytes);
        for (byte[] fieldkey : scanMap.keySet()) {
          String value = Bytes.toString(scanMap.get(fieldkey));
          rowResult.put(Bytes.toString(fieldkey), value);
          if (debug) {
            System.out.println("Result for field: " + Bytes.toString(fieldkey) + " is: " + value);
          }

        }

      }
      //add rowResult to result vector
      result.add(rowResult);
      numResults++;
      if (numResults >= recordcount) //if hit recordcount, bail out
      {
        break;
      }
    } //done with row

  } catch (IOException e) {
    if (debug) {
      System.out.println("Error in getting/parsing scan result: " + e);
    }
    return ServerError;
  } finally {
    scanner.close();
  }

  return Ok;
}

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be
   * written into the record with the specified record key, overwriting any existing values with the
   * same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public int update(String table, String key, Map<String, String> values) {
    //if this is a "new" table, init HTable object.  Else, use existing one
    if (!this.table.equals(table)) {
      hTable = null;
      try {
        getHTable(table);
        this.table = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return ServerError;
      }
    }


    if (debug) {
      System.out.println("Setting up put for key: " + key);
    }
    Put p = new Put(Bytes.toBytes(key));
    for (Map.Entry<String, String> entry : values.entrySet()) {
      if (debug) {
        System.out.println("Adding field/value " + entry.getKey() + "/" +
          entry.getValue() + " to put request");
      }
      p.add(columnFamilyBytes, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
    }

    try {
      hTable.put(p);
    } catch (IOException e) {
      if (debug) {
        System.err.println("Error doing put: " + e);
      }
      return ServerError;
    } catch (ConcurrentModificationException e) {
      //do nothing for now...hope this is rare
      return ServerError;
    }

    return Ok;
  }

  /**
   * Increment a record in the database. All fields in the specified array will be incremented by
   * one.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param fields A list of the fields to increment for the record
   * @return Zero on success, a non-zero error code on error.  See this class's description for a
   *         discussion of error codes.
   */
  @Override
  public int increment(String table, String key, List<String> fields) {
    //if this is a "new" table, init HTable object.  Else, use existing one
    if (!table.equals(table)) {
      hTable = null;
      try {
        getHTable(table);
        this.table = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return ServerError;
      }
    }


    if (debug) {
      System.out.println("Setting up increment for key: " + key);
    }
    Increment inc = new Increment(Bytes.toBytes(key));
    for (String field : fields) {
      inc.addColumn(columnFamilyBytes, Bytes.toBytes(field), 1L);
    }

    try {
      hTable.increment(inc);
    } catch (IOException e) {
      if (debug) {
        System.err.println("Error doing increment: " + e);
      }
      return ServerError;
    } catch (ConcurrentModificationException e) {
      //do nothing for now...hope this is rare
      return ServerError;
    }

    return Ok;
  }


  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be
   * written into the record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public int insert(String table, String key, Map<String, String> values) {
    return update(table, key, values);
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public int delete(String table, String key) {
    //if this is a "new" table, init HTable object.  Else, use existing one
    if (!this.table.equals(table)) {
      hTable = null;
      try {
        getHTable(table);
        this.table = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return ServerError;
      }
    }

    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }

    Delete d = new Delete(Bytes.toBytes(key));
    try {
      hTable.delete(d);
    } catch (IOException e) {
      if (debug) {
        System.err.println("Error doing delete: " + e);
      }
      return ServerError;
    }

    return Ok;
  }

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    if (args.length != 3) {
      System.out.println("Please specify a threadcount, columnfamily and operation count");
      System.exit(0);
    }

    final int keyspace = 10000; //120000000;

    final int threadcount = Integer.parseInt(args[0]);

    final String columnfamily = args[1];


    final int opcount = Integer.parseInt(args[2]) / threadcount;

    List<Callable<Integer>> tasks = Lists.newArrayList();
    for (int i = 0; i < threadcount; i++) {
      tasks.add(new LoadGenerator(opcount, keyspace, columnfamily));
    }
    ExecutorService es = Executors.newCachedThreadPool();

    long st = System.currentTimeMillis();
    List<Future<Integer>> r = es.invokeAll(tasks);
    long en = System.currentTimeMillis();

    int n = 0;
    for (Future<Integer> result : r) {
      n += result.get();
    }

    System.out.printf("Throughput: %.1f ops/sec for %d operations", ((1000.0) * (((double) (opcount * threadcount)) / ((double) (en - st)))), n);
  }

  private static class LoadGenerator implements Callable<Integer> {
    private int opCount;
    private int keySpace;
    private String columnFamily;

    public LoadGenerator(int opCount, int keySpace, String columnFamily) {
      this.opCount = opCount;
      this.keySpace = keySpace;
      this.columnFamily = columnFamily;
    }

    @Override
    public Integer call() throws DBException {
      Random random = new Random();

      HBaseClient cli = new HBaseClient();

      Properties props = new Properties();
      props.setProperty("columnfamily", columnFamily);
      props.setProperty("debug", "true");
      cli.setProperties(props);

      cli.init();

      long accum = 0;

      for (int i = 0; i < opCount; i++) {
        int keynum = random.nextInt(keySpace);
        String key = "user" + keynum;
        long st = System.currentTimeMillis();
        int rescode;

        Set<String> scanFields = Sets.newHashSet();
        scanFields.add("field1");
        scanFields.add("field3");
        List<Map<String, String>> scanResults = new ArrayList<Map<String, String>>();
        rescode = cli.scan("table1", "user2", 20, null, scanResults);

        long en = System.currentTimeMillis();

        accum += (en - st);

        if (rescode != Ok) {
          System.out.println("Error " + rescode + " for " + key);
        }

        System.out.println(i + " operations, average latency: " + (((double) accum) / ((double) i)));
      }

      //System.out.println("Average latency: "+(((double)accum)/((double)opcount)));
      //System.out.println("Average get latency: "+(((double)cli.TotalGetTime)/((double)cli.TotalGetOps)));
      return opCount;
    }
  }
}


