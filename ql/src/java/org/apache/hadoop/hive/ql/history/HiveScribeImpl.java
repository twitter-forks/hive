package org.apache.hadoop.hive.ql.history;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryStats;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.Counters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveScribeImpl implements HiveHistory {
  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.exec.HiveScribeImpl");

  String histFileName; // History file name

  private Map<String, String> idToTableMap = null;

  // Job Hash Map
  private final HashMap<String, HiveHistory.QueryInfo> queryInfoMap = new HashMap<String, HiveHistory.QueryInfo>();

  // Task Hash Map
  private final HashMap<String, HiveHistory.TaskInfo> taskInfoMap = new HashMap<String, HiveHistory.TaskInfo>();

  private static final String ROW_COUNT_PATTERN = "RECORDS_OUT_(\\d+)(_)*(\\S+)*";

  private static final Pattern rowCountPattern = Pattern.compile(ROW_COUNT_PATTERN);

  // Query Hash Map
  public static HashMap<String, QueryStats> queryStatsMap = new HashMap<String, QueryStats>();

  /**
   * Construct HiveScribeImpl object and open history log file.
   *
   * @param ss
   */
  public HiveScribeImpl(SessionState ss) {
    LOG.info("Instantiated an instance for HiveScribeImpl. Replaced hive history file at local dir\n");
    HashMap<String, String> hm = new HashMap<String, String>();
    hm.put(HiveHistory.Keys.SESSION_ID.name(), ss.getSessionId());
  }

  @Override
  public String getHistFileName() {
    histFileName = "HiveQueryCompeletionScribe";
    return String.format("%s", histFileName);
  }

  void createNewQueryMetricEntry (HiveHistory.RecordTypes rt, Map<String, String> keyValMap) {
    String QueryID = keyValMap.get("QUERY_ID");
    QueryStats newQueryStats;
    newQueryStats = new QueryStats(QueryID, keyValMap.get("QUERY_STRING"), System.currentTimeMillis(), 0);
    this.queryStatsMap.put(QueryID, newQueryStats);
  }

  void scribeCompletedQueryMetricEntry (HiveHistory.RecordTypes rt, Map<String, String> keyValMap) {
    String QueryID = keyValMap.get("QUERY_ID");
    QueryStats stats = this.queryStatsMap.get(QueryID);
    stats.queryEnd = System.currentTimeMillis();

    // Append session info from SessionState ss = SessionState.get();
    SessionState ss = SessionState.get();
    if (ss != null && ss.getUserName() != null && ss.getUserIpAddress() != null && ss.getSessionId() != null) {
      stats.username = ss.getUserName().toString();
      stats.IPAddress = ss.getUserIpAddress().toString();
      stats.sessionID = ss.getSessionId().toString();
      stats.database = ss.getCurrentDatabase().toString();
      stats.mapReduceStatsDesc = ss.getMapRedStats().toString();
      stats.currentTimeStamp = ss.getQueryCurrentTimestamp().toString();
      stats.mapReduceStats = ss.getMapRedStats();
    }

    QueryCompletedEventScriber hiveHistScriber = new QueryCompletedEventScriber();
    if (stats != null) {
      hiveHistScriber.handle(stats);
      LOG.info("Query stats passed to log pipeline");
      // Remove entry from hashmap after sending/scribing stats to log pipeline
      this.queryStatsMap.remove(QueryID);
      LOG.info("Removed Query stats from cache");
    }
    else {
      LOG.info("Stats is null. Nothing pass to log pipeline");
    }
  }

  @Override
  public void startQuery(String cmd, String id) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      return;
    }
    HiveHistory.QueryInfo queryInfo = new HiveHistory.QueryInfo();
    queryInfo.hm.put(HiveHistory.Keys.QUERY_ID.name(), id);
    queryInfo.hm.put(HiveHistory.Keys.QUERY_STRING.name(), cmd);
    queryInfoMap.put(id, queryInfo);
    createNewQueryMetricEntry(HiveHistory.RecordTypes.QueryStart, queryInfo.hm);
  }

  @Override
  public void setQueryProperty(String queryId, HiveHistory.Keys propName, String propValue) {
    HiveHistory.QueryInfo queryInfo = queryInfoMap.get(queryId);
    if (queryInfo == null) {
      return;
    }
    queryInfo.hm.put(propName.name(), propValue);
  }

  @Override
  public void setTaskProperty(String queryId, String taskId, Keys propName, String propValue) {
    String id = queryId + ":" + taskId;
    TaskInfo taskInfo = taskInfoMap.get(id);
    if (taskInfo == null) {
      return;
    }
    taskInfo.hm.put(propName.name(), propValue);
  }

  @Override
  public void setTaskCounters(String queryId, String taskId, Counters ctrs) {
    String id = queryId + ":" + taskId;
    HiveHistory.QueryInfo queryInfo = queryInfoMap.get(queryId);
    StringBuilder sb1 = new StringBuilder("");
    HiveHistory.TaskInfo taskInfo = taskInfoMap.get(id);
    if ((taskInfo == null) || (ctrs == null)) {
      return;
    }
    StringBuilder sb = new StringBuilder("");
    try {
      boolean first = true;
      for (Counters.Group group : ctrs) {
        for (Counters.Counter counter : group) {
          if (first) {
            first = false;
          } else {
            sb.append(',');
          }
          sb.append(group.getDisplayName());
          sb.append('.');
          sb.append(counter.getDisplayName());
          sb.append(':');
          sb.append(counter.getCounter());
          String tab = getRowCountTableName(counter.getDisplayName());
          if (tab != null) {
            if (sb1.length() > 0) {
              sb1.append(",");
            }
            sb1.append(tab);
            sb1.append('~');
            sb1.append(counter.getCounter());
            queryInfo.rowCountMap.put(tab, counter.getCounter());
          }
        }
      }
    } catch (Exception e) {
      LOG.warn(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    if (sb1.length() > 0) {
      taskInfoMap.get(id).hm.put(HiveHistory.Keys.ROWS_INSERTED.name(), sb1.toString());
      queryInfoMap.get(queryId).hm.put(HiveHistory.Keys.ROWS_INSERTED.name(), sb1
          .toString());
    }
    if (sb.length() > 0) {
      taskInfoMap.get(id).hm.put(HiveHistory.Keys.TASK_COUNTERS.name(), sb.toString());
    }
  }

  @Override
  public void printRowCount(String queryId) {
    return;
  }

  @Override
  public void endQuery(String queryId) {
    HiveHistory.QueryInfo queryInfo = queryInfoMap.get(queryId);
    if (queryInfo == null) {
      return;
    }
    scribeCompletedQueryMetricEntry(HiveHistory.RecordTypes.QueryEnd, queryInfo.hm);
    queryInfoMap.remove(queryId);
  }

  @Override
  public void startTask(String queryId, Task<? extends Serializable> task,
                        String taskName) {
    HiveHistory.TaskInfo taskInfo = new HiveHistory.TaskInfo();
    taskInfo.hm.put(HiveHistory.Keys.QUERY_ID.name(), queryId);
    taskInfo.hm.put(HiveHistory.Keys.TASK_ID.name(), task.getId());
    taskInfo.hm.put(HiveHistory.Keys.TASK_NAME.name(), taskName);
    String id = queryId + ":" + task.getId();
    taskInfoMap.put(id, taskInfo);
  }

  @Override
  public void endTask(String queryId, Task<? extends Serializable> task) {
    String id = queryId + ":" + task.getId();
    HiveHistory.TaskInfo taskInfo = taskInfoMap.get(id);
    if (taskInfo == null) {
      return;
    }
    taskInfoMap.remove(id);
  }

  @Override
  public void progressTask(String queryId, Task<? extends Serializable> task) {
    return;
  }

  /**
   * write out counters.
   */
  static final ThreadLocal<Map<String,String>> ctrMapFactory =
      new ThreadLocal<Map<String, String>>() {
        @Override
        protected Map<String,String> initialValue() {
          return new HashMap<String,String>();
        }
  };

  @Override
  public void logPlanProgress(QueryPlan plan) throws IOException {
    if (plan != null) {
      Map<String,String> ctrmap = ctrMapFactory.get();
      ctrmap.put("plan", plan.toString());
    }
  }

  @Override
  public void setIdToTableMap(Map<String, String> map) {
    idToTableMap = map;
  }

  /**
   * Returns table name for the counter name.
   *
   * @param name
   * @return tableName
   */
  String getRowCountTableName(String name) {
    if (idToTableMap == null) {
      return null;
    }
    Matcher m = rowCountPattern.matcher(name);

    if (m.find()) {
      String tuple = m.group(1);
      String tableName = m.group(3);
      if (tableName != null)
        return tableName;

      return idToTableMap.get(tuple);
    }
    return null;
  }

  @Override
  public void closeStream() {
    return;
  }

  @Override
  public void finalize() throws Throwable {
    closeStream();
    super.finalize();
  }
}
