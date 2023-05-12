/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.generator.UniformLongGenerator;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.generator.AlibabaGenerator.AlibabaRequest;
import com.yahoo.ycsb.generator.AlibabaGenerator.AlibabaSession;
import com.yahoo.ycsb.generator.AlibabaGenerator.AlibabaTrace;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Loads a file with a set of commands for clients to run, doing simple CRUD operations.
 * <p>
 * Properties to control the client:
 * <UL>
 * <LI><b>tracefile</b>: relative path of file
 *
 *
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just
 * one (false) (default: false)
 * </UL>
 */
public class AlibabaWorkload extends Workload {
  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

  protected String table;

  /**
   * The name of the property for the name of the tracefile.
   */
  public static final String TRACE_FILENAME_PROPERTY = "tracefilename";

  /**
   * Default name of tracefile.
   */
  public static final String TRACE_FILENAME_PROPERTY_DEFAULT = "tracefile.trace";

  /**
   * The number of threads in branching thread pool.
   */
  public static final String BRANCHING_THREAD_POOL_SIZE_PROPERTY = "branchingthreadpoolsize";

  /**
   * Default branching thread pool size.
   */
  public static final String BRANCHING_THREAD_POOL_SIZE_DEFAULT = "1";

  /**
   * The name of the property for the number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /**
   * Default number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  private List<String> fieldnames;

  /**
   * The name of the property for the field length distribution. Options are "uniform", "zipfian"
   * (favouring short records), "constant", and "histogram".
   * <p>
   * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the
   * fieldlength property. If "histogram", then the histogram will be read from the filename
   * specified in the "fieldlengthhistogram" property.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

  /**
   * The default field length distribution.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

  /**
   * The name of the property for the length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY = "fieldlength";

  /**
   * The default maximum length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

  /**
   * The name of a property that specifies the filename containing the field length histogram (only
   * used if fieldlengthdistribution is "histogram").
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";

  /**
   * The default filename containing a field length histogram.
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

  /**
   * Generator object that produces field lengths.  The value of this depends on the properties that
   * start with "FIELD_LENGTH_".
   */
  protected NumberGenerator fieldlengthgenerator;

  /**
   * The name of the property for deciding whether to read one field (false) or all fields (true) of
   * a record.
   */
  public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

  /**
   * The default value for the readallfields property.
   */
  public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

  protected boolean readallfields;

  /**
   * The name of the property for deciding whether to write one field (false) or all fields (true)
   * of a record.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

  /**
   * The default value for the writeallfields property.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

  protected boolean writeallfields;

  /**
   * The name of the property for deciding whether to check all returned
   * data against the formation template to ensure data integrity.
   */
  public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";

  /**
   * The default value for the dataintegrity property.
   */
  public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "false";

  /**
   * Set to true if want to check correctness of reads. Must also
   * be set to true during loading phase to function.
   */
  private boolean dataintegrity;

  /**
   * How many times to retry when insertion of a single item to a DB fails.
   */
  public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
  public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

  /**
   * On average, how long to wait between the retries, in seconds.
   */
  public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
  public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";

  private String traceFilename;
  protected AlibabaGenerator tracefile;
  protected NumberGenerator fieldchooser;
  protected long fieldcount;
  protected int insertionRetryLimit;
  protected int insertionRetryInterval;

  private Measurements measurements = Measurements.getMeasurements();

  protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
    NumberGenerator fieldlengthgenerator;
    String fieldlengthdistribution = p.getProperty(
        FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
    int fieldlength =
        Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
    String fieldlengthhistogram = p.getProperty(
        FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
    if (fieldlengthdistribution.compareTo("constant") == 0) {
      fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
    } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
      fieldlengthgenerator = new UniformLongGenerator(1, fieldlength);
    } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
      fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
    } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
      try {
        fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
      } catch (IOException e) {
        throw new WorkloadException(
            "Couldn't read field length histogram file: " + fieldlengthhistogram, e);
      }
    } else {
      throw new WorkloadException(
          "Unknown field length distribution \"" + fieldlengthdistribution + "\"");
    }
    return fieldlengthgenerator;
  }

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    traceFilename = p.getProperty(TRACE_FILENAME_PROPERTY, TRACE_FILENAME_PROPERTY_DEFAULT);
    tracefile = new AlibabaGenerator(traceFilename);

    branchingThreadPoolSize =
        Integer.parseInt(p.getProperty(BRANCHING_THREAD_POOL_SIZE_PROPERTY, BRANCHING_THREAD_POOL_SIZE_DEFAULT));

    fieldcount =
        Long.parseLong(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    fieldnames = new ArrayList<>();
    for (int i = 0; i < fieldcount; i++) {
      fieldnames.add("field" + i);
    }
    fieldlengthgenerator = CoreWorkload.getFieldLengthGenerator(p);

    readallfields = Boolean.parseBoolean(
        p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
    writeallfields = Boolean.parseBoolean(
        p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

    dataintegrity = Boolean.parseBoolean(
        p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
    // Confirm that fieldlengthgenerator returns a constant if data
    // integrity check requested.
    if (dataintegrity && !(p.getProperty(
        FIELD_LENGTH_DISTRIBUTION_PROPERTY,
        FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
      System.err.println("Must have constant field size to check data integrity.");
      System.exit(-1);
    }

    fieldchooser = new UniformLongGenerator(0, fieldcount - 1);

    insertionRetryLimit = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));
  }

  /**
   * Builds a value for a randomly chosen field.
   */
  private HashMap<String, ByteIterator> buildSingleValue(String key) {
    HashMap<String, ByteIterator> value = new HashMap<>();

    String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
    ByteIterator data;
    if (dataintegrity) {
      data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
    } else {
      // fill with random data
      data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
    }
    value.put(fieldkey, data);

    return value;
  }

  /**
   * Builds values for all fields.
   */
  private HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<>();

    for (String fieldkey : fieldnames) {
      ByteIterator data;
      if (dataintegrity) {
        data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
      } else {
        // fill with random data
        data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
      }
      values.put(fieldkey, data);
    }
    return values;
  }

  /**
   * Build a deterministic value given the key information.
   */
  private String buildDeterministicValue(String key, String fieldkey) {
    int size = fieldlengthgenerator.nextValue().intValue();
    StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);

    return sb.toString();
  }

  /**
   * There is no load phase for trace files.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    AlibabaSession nextSession = tracefile.nextValue();

    // we reached the end of the trace
    if(nextSession == null) {
      return false;
    }

    // writes
    for (AlibabaTrace trace : nextSession.getTraces()) {
      for (AlibabaRequest request : trace.getRequests()) {
        if (request.isRead()) {
          doTransactionInsert(db, request.getObjId());
        }
      }
    }

    return true;
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    AlibabaSession nextSession = tracefile.nextValue();

    // we reached the end of the trace
    if(nextSession == null) {
      return false;
    }

    for (AlibabaTrace trace : nextSession.getTraces()) {
      traverseRequests(db, trace.getRootRequest());
    }

    return true;
  }

  private void traverseRequests(DB db, AlibabaRequest request){
    if (request.isStateful()){
      // AlibabaRequest [
      //   timestamp=1937,
      //   rpcid=0.1.1.2.1.9,
      //   dm=d3a0dbd99cef0ff255b05ad66d6c56af5ed7b6b507baafa096b22e8b0e8eed08,
      //   op=READ,
      //   objId=420906802748
      // ]

      String operation = request.getOp().name();
      String key = request.getObjId();
      switch (operation) {
      case "READ":
        doTransactionRead(db, key);
        break;
      case "WRITE":
        doTransactionInsert(db, key);
        break;
      default:
        break;
      }
    }

    try {
      List<Future<Void>> futures = new ArrayList<>();
      for (AlibabaRequest dep : request.getDependencies()) {
        Future<Void> f = new AsyncTraverseRequest().call(db, dep);
        futures.add(f);
        f.get(); // <-- REMOVE LATER -- PROBLEMS WITH CONCURRENT JEDIS CONNECTIONS
      }
      // wait for all dependencies
      for (Future<Void> f : futures) {
        f.get();
      }
    } catch (java.lang.InterruptedException e) {
      throw new RuntimeException(e);
    } catch (java.util.concurrent.ExecutionException e) {
      throw new RuntimeException(e);
    }

    // only return main future when its dependencies also finish
    return;
  }

  /**
   * Execute database operation in a thread.
   */
  public class AsyncTraverseRequest {
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public Future<Void> call(DB db, AlibabaRequest dep) {
      return executor.submit(() -> {
          traverseRequests(db, dep);
          return null;
        });
    }
  }

  /**
   * Results are reported in the first three buckets of the histogram under
   * the label "VERIFY".
   * Bucket 0 means the expected data was returned.
   * Bucket 1 means incorrect data was returned.
   * Bucket 2 means null data was returned when some data was expected.
   */
  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.OK;
    long startTime = System.nanoTime();
    if (!cells.isEmpty()) {
      for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        if (!entry.getValue().toString().equals(buildDeterministicValue(key, entry.getKey()))) {
          verifyStatus = Status.UNEXPECTED_STATE;
          break;
        }
      }
    } else {
      // This assumes that null data is never valid
      verifyStatus = Status.ERROR;
    }
    long endTime = System.nanoTime();
    measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
    measurements.reportStatus("VERIFY", verifyStatus);
  }

  public void doTransactionRead(DB db, String keyname) {
    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    } else if (dataintegrity) {
      // pass the full field list if dataintegrity is on for verification
      fields = new HashSet<String>(fieldnames);
    }

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
  }

  public void doTransactionReadModifyWrite(DB db, String keyname) {
    // choose a random key
    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    // do the transaction

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();


    long ist = measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    db.read(table, keyname, fields, cells);

    db.update(table, keyname, values);

    long en = System.nanoTime();

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }

    measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
    measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
  }

  public void doTransactionInsert(DB db, String dbkey) {
    HashMap<String, ByteIterator> values = buildValues(dbkey);
    db.insert(table, dbkey, values);
  }
}
