package org.apache.hadoop.hive.ql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * QueryStats.
 */
public class QueryStats {
  private long startTime;
  private long endTime;
  private String queryId;
  private String queryString;
  private String sessionID;
  private String IPAddress;
  private String username;
  private String database;
  private String mapReduceStatsDesc;
  private String currentTimeStamp;
  private ArrayList<task> taskProgress;
  private ArrayList<plan> planProgress;
  private Map<String, MapRedStats> mapReduceStats;

  public static class task {
    private Long timeStamp;
    private String progress;

    public Long getTimeStamp() {
      return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
      this.timeStamp = timeStamp;
    }

    public String getProgress() {
      return progress;
    }

    public void setProgress(String progress) {
      this.progress = progress;
    }
  }

  public static class plan {
    private Long timeStamp;
    private QueryPlan queryPlan;

    public Long getTimeStamp() {
      return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
      this.timeStamp = timeStamp;
    }

    public QueryPlan getQueryPlan() {
      return queryPlan;
    }

    public void setQueryPlan(QueryPlan queryPlan) {
      this.queryPlan = queryPlan;
    }
  }

  public QueryStats(String queryID, String queryString, Long queryStart) {
    assert (queryID != null && queryString != null && queryStart != null) : "Invalid query entry. Failed to create a new QueryStats entry.";
    this.queryId = queryID;
    this.queryString = queryString;
    this.startTime = queryStart;
    this.endTime = -1;
    this.taskProgress = new ArrayList<>();
    this.planProgress = new ArrayList<>();
    this.mapReduceStats = new HashMap<>();
  }

  public void setStartTime(Long timeStamp) {
    this.startTime = timeStamp;
  }

  public Long getStartTime() {
    return this.startTime;
  }

  public void setEndTime(Long timeStamp) {
    this.endTime = timeStamp;
  }

  public Long getEndTime() {
    return this.endTime;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public String getQueryId() {
    return this.queryId;
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

  public ArrayList<task> getTaskProgress() {
    return this.taskProgress;
  }

  public ArrayList<plan> getPlanProgress() {
    return this.planProgress;
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
