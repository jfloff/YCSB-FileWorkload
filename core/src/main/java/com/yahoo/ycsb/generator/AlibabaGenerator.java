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
import java.util.ArrayList;
import java.util.Arrays;
import com.google.gson.*;
import com.google.gson.annotations.*;
import com.yahoo.ycsb.Utils;

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
      // 0 is clientId which should be the same for the whole file
      System.err.println(csvRow[1]);
      this.id = csvRow[1];
      this.ts = Long.parseLong(csvRow[2]);
      // parse the array of traces
      Gson gson = new GsonBuilder()
          .create();
      this.traces = Arrays.asList(gson.fromJson(csvRow[3], AlibabaTrace[].class));
      // fill the connections after loading the file
      for (AlibabaTrace trace : this.traces) {
        trace.init();
      }
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

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((id == null) ? 0 : id.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj){
        return true;
      }
      if (obj == null){
        return false;
      }
      if (getClass() != obj.getClass()){
        return false;
      }
      AlibabaSession other = (AlibabaSession) obj;
      if (id == null) {
        if (other.id != null){
          return false;
        }
      } else if (!id.equals(other.id)){
        return false;
      }
      return true;
    }
  }

  /**
   * Represents an Alibaba Trace entry.
   */
  public static class AlibabaTrace {
    private String traceid;
    private AlibabaRequest rootRequest;
    private List<AlibabaRequest> requests;

    public void init() {
      // fill the traceid in every request
      for (AlibabaRequest request : requests) {
        request.setTraceid(this.traceid);

        // set root request
        if (request.isRoot()){
          this.rootRequest = request;
        }
      }

      // fill the graph operations
      for (AlibabaRequest request : requests) {
        for (AlibabaRequest n : requests) {
          // skip same request
          if (n == request) {
            continue;
          }

          // compare the rpcid prefix with the whole request rpcid
          if (Utils.rpartition(n.getRpcid(), '.')[0].equals(request.getRpcid())) {
            request.addDependency(n);
          }
        }
      }
    }

    public String getTraceid() {
      return traceid;
    }

    public AlibabaRequest getRootRequest() {
      return rootRequest;
    }

    public List<AlibabaRequest> getRequests() {
      return requests;
    }

    @Override
    public String toString() {
      // return "AlibabaTrace [traceid=" + traceid + "]";
      return "AlibabaTrace [traceid=" + traceid + ", requests=" + requests + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((traceid == null) ? 0 : traceid.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj){
        return true;
      }
      if (obj == null){
        return false;
      }
      if (getClass() != obj.getClass()){
        return false;
      }
      AlibabaTrace other = (AlibabaTrace) obj;
      if (traceid == null) {
        if (other.traceid != null){
          return false;
        }
      } else if (!traceid.equals(other.traceid)){
        return false;
      }
      return true;
    }
  }

  /**
   * Represents an Alibaba Request entry.
   */
  public static class AlibabaRequest {
    private String traceid;
    private Long timestamp;
    private String rpcid;
    private String dm;
    @SerializedName("obj_id")
    private String objId;
    private Operation op;
    private List<AlibabaRequest> dependencies = new ArrayList<>();

    /**
     * Represents datastore operation over the object.
     */
    public enum Operation {
      @SerializedName("r")
      READ,
      @SerializedName("w")
      WRITE
    }

    public Long getTimestamp() {
      return timestamp;
    }

    public String getRpcid() {
      return rpcid;
    }

    public String getDm() {
      return dm;
    }

    public Operation getOp() {
      return op;
    }

    public String getObjId() {
      return objId;
    }

    public String getTraceid() {
      return traceid;
    }

    public void setTraceid(String t) {
      this.traceid = t;
    }

    public List<AlibabaRequest> getDependencies() {
      return dependencies;
    }

    // helpers
    public boolean isRoot() {
      return this.rpcid.equals("0");
    }

    public boolean isRead() {
      return (op == Operation.READ);
    }

    public boolean isWrite() {
      return (op == Operation.WRITE);
    }

    public boolean isStateless() {
      return (op == null);
    }

    public boolean isStateful() {
      return !isStateless();
    }

    // graph
    public void addDependency(AlibabaRequest r){
      this.dependencies.add(r);
    }

    @Override
    public String toString() {
      return "AlibabaRequest [traceid=" + traceid + ", timestamp=" + timestamp + ", rpcid=" + rpcid +
          ", dm=" + dm + ", objId=" + objId + ", op=" + op + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((traceid == null) ? 0 : traceid.hashCode());
      result = prime * result + ((rpcid == null) ? 0 : rpcid.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj){
        return true;
      }
      if (obj == null){
        return false;
      }
      if (getClass() != obj.getClass()){
        return false;
      }
      AlibabaRequest other = (AlibabaRequest) obj;
      if (traceid == null) {
        if (other.traceid != null){
          return false;
        }
      } else if (!traceid.equals(other.traceid)){
        return false;
      }
      if (rpcid == null) {
        if (other.rpcid != null){
          return false;
        }
      } else if (!rpcid.equals(other.rpcid)){
        return false;
      }
      return true;
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
