/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.generator;

import java.io.FileReader;
import java.io.IOException;
import com.opencsv.*;
import java.util.List;
import java.util.Arrays;
import com.google.gson.Gson;
import com.google.gson.annotations.*;

/**
 * A generator, whose sequence is the lines of a file.
 */
public class AlibabaGenerator extends Generator<AlibabaGenerator.AlibabaSession> {
  /**
   * Represents an Alibaba Session entry.
   */
  public static class AlibabaSession {
    private String id;
    private Long ts;
    private List<AlibabaTrace> traces;

    public AlibabaSession(String[] csvRow) {
      this.id = csvRow[0];
      this.ts = Long.parseLong(csvRow[1]);
      // parse the array of traces
      Gson gson = new Gson();
      this.traces = Arrays.asList(gson.fromJson(csvRow[2], AlibabaTrace[].class));
    }

    public String getId() {
      return id;
    }

    public Long getTs() {
      return ts;
    }

    public List<AlibabaTrace> getTraces() {
      return traces;
    }

    @Override
    public String toString() {
      return "AlibabaSession [id=" + id + ", ts=" + ts + ", traces=" + traces + "]";
    }
  }

  /**
   * Represents an Alibaba Trace entry.
   */
  public static class AlibabaTrace {
    private String traceid;
    private List<AlibabaRequest> requests;

    /**
    *
    * @param traceid
    * @param requests
    */
    public AlibabaTrace(String traceid, List<AlibabaRequest> requests) {
      this.traceid = traceid;
      this.requests = requests;
    }

    public String getTraceid() {
      return traceid;
    }

    public List<AlibabaRequest> getRequests() {
      return requests;
    }

    @Override
    public String toString() {
      // return "AlibabaTrace [traceid=" + traceid + "]";
      return "AlibabaTrace [traceid=" + traceid + ", requests=" + requests + "]";
    }
  }

  /**
   * Represents an Alibaba Request entry.
   */
  public static class AlibabaRequest {
    private Long timestamp;
    private String rpcid;
    private String rpctype;
    private String dm;
    private String um;
    private Long objId;
    private Operation op;

    /**
     * Represents datastore operation over the object.
     */
    public enum Operation {
      @SerializedName("r")
      READ,
      @SerializedName("w")
      WRITE
    }

    public AlibabaRequest(Long timestamp, String rpcid, String rpctype,
        String dm, String um, Operation op, Long objId) {
      this.timestamp = timestamp;
      this.rpcid = rpcid;
      this.rpctype = rpctype;
      this.dm = dm;
      this.um = um;
      this.op = op;
      this.objId = objId;
    }

    public Long getTimestamp() {
      return timestamp;
    }

    public String getRpcid() {
      return rpcid;
    }

    public String getRpctype() {
      return rpctype;
    }

    public String getDm() {
      return dm;
    }

    public String getUm() {
      return um;
    }

    public Operation getOp() {
      return op;
    }

    public Long getObjId() {
      return objId;
    }

    // helpers
    public boolean isRead() {
      return (op == Operation.READ);
    }

    public boolean isWrite() {
      return (op == Operation.WRITE);
    }

    public boolean toStateless() {
      return (op == null);
    }

    public boolean toStateful() {
      return !toStateless();
    }

    @Override
    public String toString() {
      return "AlibabaRequest [timestamp=" + timestamp + ", rpcid=" + rpcid + ", rpctype=" + rpctype + ", dm=" + dm
          + ", um=" + um + ", op=" + op + ", objId=" + objId + "]";
    }
  }


  /**
   * Alibaba Generator.
   */
  private final String filename;
  private AlibabaSession current;
  private CSVReader reader;

  /**
   * Create a FileGenerator with the given file.
   * @param filename The file to read lines from.
   */
  public AlibabaGenerator(String filename) {
    this.filename = filename;
    reloadFile();
  }

  /**
   * Return the next string of the sequence, ie the next line of the file.
   */
  @Override
  public synchronized AlibabaSession nextValue() {
    try {
      String[] nextJson = reader.readNext();
      if (nextJson == null) {
        current = null;
      } else {
        current = new AlibabaSession(nextJson);
      }
      return current;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (com.opencsv.exceptions.CsvValidationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return the previous read line.
   */
  @Override
  public AlibabaSession lastValue() {
    return current;
  }

  /**
   * Reopen the file to reuse values.
   */
  public synchronized void reloadFile() {
    try {
      System.err.println("Reload " + filename);
      reader = new CSVReader(new FileReader(filename));
      // skip title
      reader.skip(1);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}
