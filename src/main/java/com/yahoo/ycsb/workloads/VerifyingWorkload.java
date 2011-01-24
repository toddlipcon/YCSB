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

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
 * relative proportion of different kinds of operations, and other properties of the workload, are
 * controlled by parameters specified at runtime.
 * <p/>
 * Properties to control the client: <UL> <LI><b>fieldcount</b>: the number of fields in a record
 * (default: 10) <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just
 * one (false) (default: false) <LI><b>readproportion</b>: what proportion of operations should be
 * reads (default: 0.95) <LI><b>updateproportion</b>: what proportion of operations should be
 * updates (default: 0.05) <LI><b>insertproportion</b>: what proportion of operations should be
 * inserts (default: 0) <LI><b>scanproportion</b>: what proportion of operations should be scans
 * (default: 0) <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a
 * record, modify it, write it back (default: 0) <LI><b>requestdistribution</b>: what distribution
 * should be used to select the records to operate on - uniform, zipfian or latest (default:
 * uniform) <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan
 * (default: 1000) <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to
 * choose the number of records to scan, for each scan, between 1 and maxscanlength (default:
 * uniform) <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in
 * hashed order ("hashed") (default: hashed) </ul>
 */
public class VerifyingWorkload extends CoreWorkload {
  private volatile int errorCount = 0;
  private volatile int errorLogRate = 1;

  private AtomicInteger insertKey = new AtomicInteger(0);

  public VerifyingWorkload(Properties p) throws WorkloadException {
    super();
    super.init(p);
  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads,
   * this function must be thread safe. However, avoid synchronized, or the threads will block
   * waiting for each other, and it will be difficult to reach the target throughput. Ideally, this
   * function would have no side effects other than DB operations.
   * <p/>
   * Note that the data inserted is generated from the key number and field so it can be checked
   * later.
   */
  public boolean doInsert(DB db, Object threadState) {
    doTransactionInsert(db);
    return true;
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will
   * block waiting for each other, and it will be difficult to reach the target throughput. Ideally,
   * this function would have no side effects other than DB operations.
   */
  public boolean doTransaction(DB db, Object threadState) {
    String op = operationchooser.nextString();

    if (op.compareTo("READ") == 0) {
      doTransactionRead(db);
    } else if (op.compareTo("UPDATE") == 0) {
      doTransactionUpdate(db);
    } else if (op.compareTo("INSERT") == 0) {
      doTransactionInsert(db);
    } else if (op.compareTo("SCAN") == 0) {
      doTransactionScan(db);
    } else {
      doTransactionReadModifyWrite(db);
    }

    return true;
  }

  public void doTransactionInsert(DB db) {
    //choose the next key
    int keynum = insertKey.getAndIncrement();
    if (!orderedinserts) {
      keynum = Utils.hash(keynum);
    }

    Map<String, String> values = new HashMap<String, String>();
    for (int i = 0; i < fieldcount; i++) {
      values.put(String.format("field%05d", i), Utils.testableData(keynum, i, fieldlength));
    }
    reportErrorIfAny("insert", db.insert(table, "user" + keynum, values));
  }

  public void doTransactionRead(DB db) {
    readAndCheckRecord(db, chooseKey(), "read");
  }

  public void doTransactionReadModifyWrite(DB db) {
    int keyNum = chooseKey();

    String keyname = "user" + keyNum;

    Set<String> fields;

    readAndCheckRecord(db, keyNum, "READ-modify-write");

    if (!readallfields) {
      //read a random field
      String fieldname = String.format("field%05d", fieldchooser.nextInt());


      fields = new HashSet<String>();
      fields.add(fieldname);
    } else {
      fields = null;
    }

    Map<String, String> values = new HashMap<String, String>();

    if (writeallfields) {
      //new data for all the fields
      for (int i = 0; i < fieldcount; i++) {
        String fieldName = String.format("field%05d", i);
        String data = Utils.testableData(keyNum, i, fieldlength);
        values.put(fieldName, data);
      }
    } else {
      //update a random field
      int fieldNumber = fieldchooser.nextInt();
      String fieldName = String.format("field%05d", fieldNumber);
      String data = Utils.testableData(keyNum, fieldNumber, fieldlength);
      values.put(fieldName, data);
    }

    //do the transaction

    long st = System.currentTimeMillis();

    reportErrorIfAny("read-modify-WRITE", db.update(table, keyname, values));

    long en = System.currentTimeMillis();

    Measurements.getMeasurements().measure("READ-MODIFY-WRITE", (int) (en - st));
  }

  public void doTransactionScan(DB db) {
    //choose a random key
    int keyNum = chooseKey();
    String startKeyName = "user" + keyNum;

    //choose a random scan length
    int len = fieldchooser.nextInt();

    Set<String> fields = null;

    if (!readallfields) {
      //read a random field
      fields = new HashSet<String>();
      fields.add(String.format("field%05d", fieldchooser.nextInt()));
    }

    List<Map<String, String>> result = new ArrayList<Map<String, String>>();
    reportErrorIfAny("scan", db.scan(table, startKeyName, len, fields, result));
    for (Map<String, String> row : result) {
      for (String field : row.keySet()) {
        if (!field.matches("field[0-9]+") || !row.get(field).matches("[0-9]+-.*")) {
          throw new DataCorruptionException(String.format("Bad data found in field %s", field));
        }
      }
    }
  }

  public void doTransactionUpdate(DB db) {
    //choose a random key
    int keyNum = chooseKey();
    String keyname = "user" + keyNum;

    Map<String, String> values = new HashMap<String, String>();

    if (writeallfields) {
      //new data for all the fields
      for (int i = 0; i < fieldcount; i++) {
        String fieldName = String.format("field%05d", i);
        String data = Utils.ASCIIString(fieldlength);
        values.put(fieldName, data);
      }
    } else {
      //update a random field
      int fieldNum = fieldchooser.nextInt();

      values.put(String.format("field%05d", fieldNum), Utils.testableData(keyNum, fieldNum, fieldlength));
    }

    reportErrorIfAny("update", db.update(table, keyname, values));
  }

  private int chooseKey() {
    //choose a random key
    int keyNum;
    do {
      keyNum = keychooser.nextInt() % insertKey.get();
    }
    while (keyNum < 0 || keyNum > insertKey.get());

    if (!orderedinserts) {
      keyNum = Utils.hash(keyNum);
    }
    return keyNum;
  }

  private void readAndCheckRecord(DB db, int keyNum, String type) {
    String keyName = "user" + keyNum;

    Set<String> fields = null;

    if (!readallfields) {
      //read a random field
      fields = new HashSet<String>();
      fields.add(String.format("field%05d", fieldchooser.nextInt()));
    }

    Map<String, String> result = new HashMap<String, String>();
    int errorCode = db.read(table, keyName, fields, result);

    if (!verify(keyNum, result, errorCode, type)) {
      throw new DataCorruptionException(String.format("Data corrupted for key %s, value = %s", keyName, result));
    }
  }

  private boolean verify(int keyNum, Map<String, String> result, int errorCode, String type) {
    reportErrorIfAny(type, errorCode);
    for (String field : result.keySet()) {
      int fieldNumber = Integer.parseInt(field.substring(5));
      if (!Utils.checkData(keyNum, fieldNumber, result.get(field))) {
        return false;
      }
    }
    return true;
  }

  private void reportErrorIfAny(String type, int errorCode) {
    if (errorCode != 0) {
      errorCount++;
      if (errorCount % errorLogRate == 0) {
        System.out.printf("%d %s errors so far\n", errorCount, type);
      }
      if (errorCount > 10 * errorLogRate) {
        errorLogRate = 10 * errorLogRate;
      }
    }
  }
}
