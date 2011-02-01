package com.yahoo.ycsb.workloads;

import com.google.common.collect.*;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.*;

public class VerifyingWorkloadTest extends TestCase {

  @Test
  public void testDataCorruptionIsDetected() throws InterruptedException {
    String s1 = Utils.testableData(1, 2, 20);
    String s2 = Utils.testableData(1, 2, 20);
    assertFalse(s1.equals(s2));

    assertTrue(Utils.checkData(1, 2, s1));
    assertTrue(Utils.checkData(1, 2, s2));
    assertFalse(Utils.checkData(1, 2, s2 + "0"));
    assertFalse(Utils.checkData(1, 2, s2.substring(1)));
    assertFalse(Utils.checkData(1, 2, ""));
    assertFalse(Utils.checkData(1, 2, s2.replaceFirst("-", "")));
    assertFalse(Utils.checkData(1, 2, "-" + s2));
  }

  @Test
  public void testDoTransactionInsert() throws WorkloadException {
    MemoryDB db = buildDatabase(buildWorkload());

    assertEquals(1, db.data.size());
    TreeMap<String, Map<String, String>> usertable = db.data.get("usertable");
    assertEquals(4, usertable.size());
    for (String key : usertable.keySet()) {
      assertTrue(key.matches("user.*"));
      Map<String, String> row = usertable.get(key);
      assertEquals(10, row.size());
      for (String field : row.keySet()) {
        assertTrue(field.matches("field[0-9]+"));
        assertTrue(Utils.checkData(Integer.parseInt(key.substring(4)), Integer.parseInt(field.substring(5)), row.get(field)));
      }
    }
  }

  @Test
  public void testDoTransactionRead() throws WorkloadException {
    VerifyingWorkload wl = buildWorkload();
    MemoryDB db = buildDatabase(wl);

    for (int i = 0; i < 20; i++) {
      wl.doTransactionRead(db);
    }
  }

  @Test
  public void testDoTransactionReadModifyWrite() throws WorkloadException {
    VerifyingWorkload wl = buildWorkload();
    MemoryDB db = buildDatabase(wl);
    MemoryDB checkPoint = db.copy();

    wl.doTransactionReadModifyWrite(db);

    assertEquals(1, countDifferences(db, checkPoint));

    for (int i = 0; i < 1000; i++) {
      wl.doTransactionReadModifyWrite(db);
    }

    assertEquals(40, countDifferences(db, checkPoint));

    wl = buildWorkload("writeallfields", "true");
    db = buildDatabase(wl);
    checkPoint = db.copy();

    wl.doTransactionReadModifyWrite(db);
    assertEquals(10, countDifferences(db, checkPoint));
  }

  private int countDifferences(MemoryDB db, MemoryDB checkPoint) {
    int diffCount = 0;
    assertSetEquals(db.data.keySet(), checkPoint.data.keySet());
    for (String tableKey : db.data.keySet()) {
      Map<String, Map<String, String>> table = db.data.get(tableKey);
      TreeMap<String, Map<String, String>> otherTable = checkPoint.data.get(tableKey);
      assertSetEquals(table.keySet(), otherTable.keySet());

      for (String rowKey : table.keySet()) {
        Map<String, String> row = table.get(rowKey);
        Map<String, String> otherRow = otherTable.get(rowKey);
        assertSetEquals(row.keySet(), otherRow.keySet());
        for (String field : row.keySet()) {
          if (!row.get(field).equals(otherRow.get(field))) {
            diffCount++;
          }
        }
      }
    }
    return diffCount;
  }

  private void assertSetEquals(Set<String> s1, Set<String> s2) {
    assertEquals(0, Sets.difference(s1, s2).size());
    assertEquals(0, Sets.difference(s2, s1).size());
  }

  @Test
  public void testDoTransactionScan() throws WorkloadException {
    VerifyingWorkload wl = buildWorkload("scanlength", "3");
    MemoryDB db = buildDatabase(wl);

    // hard to test the scan functionality because we don't know where it will start
    // and we don't get the keys for the returned rows which makes it impossible to test
    // the returned data.
    wl.doTransactionScan(db);

    db.data.get("usertable").firstEntry().getValue().put("bogus-field", "foobar");

    try {
      for (int i = 0; i < 10; i++) {
        wl.doTransactionScan(db);
      }
      fail("Expected data corruption exception");
    } catch (DataCorruptionException e) {
      // ignore
    }
  }

  @Test
  public void testDoTransactionUpdate() {
  }

  /**
   * Builds an in memory data-base using the specified workload generator.
   * @param wl The workload that should update the db
   * @return The database that was constructed and updated.
   * @throws WorkloadException If data doesn't verify.
   */
  private MemoryDB buildDatabase(VerifyingWorkload wl) throws WorkloadException {
    MemoryDB db = new MemoryDB();
    db.createTable("usertable");
    wl.doTransactionInsert(db);
    wl.doTransactionInsert(db);
    wl.doTransactionInsert(db);
    wl.doTransactionInsert(db);
    return db;
  }

  /**
   * Builds a verifying workload generator with specfied properties.  As a side effect the measurement infrastructure is
   * initialized as well.
   * @param properties Alternating key, value pairs for properties to set.
   * @return The workload we constructed.
   */
  private VerifyingWorkload buildWorkload(String... properties) throws WorkloadException {
    Properties p = new Properties();
    p.setProperty("recordcount", "100");
    for (int i = 0; i < properties.length; i += 2) {
      p.setProperty(properties[i], properties[i + 1]);
    }
    Measurements.setProperties(p);

    return new VerifyingWorkload(p);
  }

  /** Provides DB that we can use to verify operations. */
  private static class MemoryDB extends DB {
    int scanCount = 0;

    // table -> (key -> (field -> value))
    private Map<String, TreeMap<String, Map<String, String>>> data = Maps.newTreeMap();

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int read(String table, String key, Iterable<String> fields, Map<String, String> result) {
      result.clear();
      TreeMap<String, Map<String, String>> tableMap = data.get(table);
      if (tableMap == null) {
        return ResultCodes.NO_SUCH_TABLE.ordinal();
      }
      Map<String, String> cell = tableMap.get(key);
      if (cell == null) {
        return ResultCodes.NO_SUCH_KEY.ordinal();
      }
      result.putAll(cell);
      if (fields != null) {
        result.keySet().retainAll(Sets.newHashSet(fields));
      }
      return ResultCodes.OK.ordinal();
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored
     * in a HashMap.
     * @param table       The name of the table
     * @param startkey    The record key of the first record to read.
     * @param recordCount The number of records to read
     * @param fields      The list of fields to read, or null for all of them
     * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     *         codes.
     */
    @Override
    public int scan(String table, String startkey, int recordCount, Iterable<String> fields, List<Map<String, String>> result) {
      TreeMap<String, Map<String, String>> tableMap = data.get(table);
      if (tableMap == null) {
        return ResultCodes.NO_SUCH_TABLE.ordinal();
      }
      for (String key : tableMap.tailMap(startkey).keySet()) {
        if (recordCount <= 0) {
          break;
        }
        recordCount--;
        scanCount++;

        Map<String, String> tmp = Maps.newHashMap(tableMap.get(key));
        if (fields != null) {
          tmp.keySet().retainAll(Sets.newHashSet(fields));
        }

        result.add(tmp);
      }
      return ResultCodes.OK.ordinal();
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
     * record with the specified record key, overwriting any existing values with the same field name.
     * @param table  The name of the table
     * @param key    The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     *         codes.
     */
    @Override
    public int update(String table, String key, Map<String, String> values) {
      Map<String, Map<String, String>> tableMap = data.get(table);
      if (tableMap == null) {
        return ResultCodes.NO_SUCH_TABLE.ordinal();
      }
      Map<String, String> cell = tableMap.get(key);
      if (cell == null) {
        return ResultCodes.NO_SUCH_KEY.ordinal();
      }

      for (String field : values.keySet()) {
        cell.put(field, values.get(field));
      }
      return ResultCodes.OK.ordinal();
    }

    @Override
    public int increment(String table, String key, List<String> fields) {
      Map<String, String> results = Maps.newHashMap();
      int code = read(table, key, fields, results);
      if (code != 0) {
        return code;
      }
      for (String k : results.keySet()) {
        results.put(k, "" + (Integer.parseInt(results.get(k)) + 1));
      }
      code = update(table, key, results);
      if (code != 0) {
        return code;
      }
      return ResultCodes.OK.ordinal();
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
     * record with the specified record key.
     * @param table  The name of the table
     * @param key    The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     *         codes.
     */
    @Override
    public int insert(String table, String key, Map<String, String> values) {
      Map<String, Map<String, String>> tableMap = data.get(table);
      if (tableMap == null) {
        return ResultCodes.NO_SUCH_TABLE.ordinal();
      }
      tableMap.put(key, Maps.newLinkedHashMap(values));
      return ResultCodes.OK.ordinal();
    }

    /**
     * Delete a record from the database.
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     *         codes.
     */
    @Override
    public int delete(String table, String key) {
      Map<String, Map<String, String>> tableMap = data.get(table);
      if (tableMap == null) {
        return ResultCodes.NO_SUCH_TABLE.ordinal();
      }
      tableMap.remove(key);
      return ResultCodes.OK.ordinal();
    }

    public void createTable(String name) {
      data.put(name, Maps.<String, Map<String, String>>newTreeMap());
    }

    /**
     * Does a deep copy of a database.
     * @return A copy of the database that shares no values.
     */
    public MemoryDB copy() {
      MemoryDB r = new MemoryDB();
      for (String tableKey : data.keySet()) {
        TreeMap<String, Map<String, String>> table = data.get(tableKey);
        r.createTable(tableKey);
        for (String rowKey : table.keySet()) {
          Map<String, String> row = table.get(rowKey);
          Map<String, String> newRow = Maps.newHashMap();
          newRow.putAll(row);
          r.insert(tableKey, rowKey, newRow);
        }
      }
      return r;
    }
  }

  enum ResultCodes {
    OK, NO_SUCH_TABLE, NO_SUCH_KEY;
  }
}
