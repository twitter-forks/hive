package org.apache.hadoop.hive.ql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * QueryStats.
 */
public class QueryStats {
  private long queryStart;
  private long queryEnd;
  private String queryID;
  private String queryString;
  private String sessionID;
  private String IPAddress;
  private String username;
  private String database;
  private String mapReduceStatsDesc;
  private String currentTimeStamp;
  private ArrayList<progressSnapshot> taskProgress;
  private Map<String, QueryPlan> plansInfo;
  private Map<String, MapRedStats> mapReduceStats;

  public static class progressSnapshot {
    private long timeStamp;
    private String progress;

    public long getTimeStamp() {
      return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
      this.timeStamp = timeStamp;
    }

    public String getProgress() {
      return progress;
    }

    public void setProgress(String progress) {
      this.progress = progress;
    }
  }

  public QueryStats(String queryID, String queryString, long start) {
    this.queryID = queryID;
    this.queryString = queryString;
    this.queryStart = start;
    this.queryEnd = -1;
    this.taskProgress = new ArrayList<>();
    this.plansInfo = new HashMap<>();
    this.mapReduceStats = new HashMap<>();
  }

  public void setQueryStart(Long timeStamp) {
    this.queryStart = timeStamp;
  }

  public Long getQueryStart() {
    return this.queryStart;
  }

  public void setQueryEnd(Long timeStamp) {
    this.queryEnd = timeStamp;
  }

  public Long getQueryEnd() {
    return this.queryEnd;
  }

  public void setQueryID(String queryID) {
    this.queryID = queryID;
  }

  public String getQueryID() {
    return this.queryID;
  }

  public void setQueryString(String queryString) {
    this.queryString = queryString;
  }

  public String getQueryString() {
    return this.queryString;
  }

  public void setSessionID(String sessionID) {
    this.sessionID = sessionID;
  }

  public String getSessionID() {
    return this.sessionID;
  }

  public void setIPAddress(String IPAddress) {
    this.IPAddress = IPAddress;
  }

  public String getIPAddress() {
    return this.IPAddress;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getUsername() {
    return this.username;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getDatabase() {
    return this.database;
  }

  public void setMapReduceStatsDesc(String MapReduceStatsDesc) {
    this.mapReduceStatsDesc = MapReduceStatsDesc;
  }

  public String getMapReduceStatsDesc() {
    return this.mapReduceStatsDesc;
  }

  public void setCurrentTimeStamp(String currentTimeStamp) {
    this.currentTimeStamp = currentTimeStamp;
  }

  public String getCurrentTimeStamp() {
    return this.currentTimeStamp;
  }

  public ArrayList<progressSnapshot> getTaskProgress() {
    return this.taskProgress;
  }

  public Map<String, QueryPlan> getPlansInfo() {
    return this.plansInfo;
  }

  public Map<String, MapRedStats> getMapReduceStats() {
    return this.mapReduceStats;
  }

  public void setMapReduceStats(Map<String, MapRedStats> mapReduceStats) {
    for (Map.Entry<String, MapRedStats> ent : mapReduceStats.entrySet()) {
      this.mapReduceStats.put(ent.getKey(), ent.getValue());
    }
  }

};
