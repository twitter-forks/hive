package org.apache.hadoop.hive.ql;

import java.util.Map;

/**
 * QueryStats.
 *
 */
public class QueryStats {
  public String queryString;
  public long queryStart;
  public long queryEnd;
  public String sessionID;
  public String IPAddress;
  public String username;
  public String database;
  public String mapReduceStatsDesc;
  public String queryID;
  public String currentTimeStamp;

  // map-reduce job stats based on stage IDs
  public Map<String, MapRedStats> mapReduceStats;

  public QueryStats (String queryID, String queryString, long start, long end) {
    this.queryID = queryID;
    this.queryString = queryString;
    this.queryStart = start;
    this.queryEnd = end;
  };
};
