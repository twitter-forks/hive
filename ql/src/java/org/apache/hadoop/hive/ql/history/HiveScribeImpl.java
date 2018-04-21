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
   * @param sessionState
   */
  public HiveScribeImpl(SessionState sessionState) {
    LOG.info("Instantiated an instance for HiveScribeImpl. Replaced hive history file at local dir\n");
    HashMap<String, String> eventStats = new HashMap<String, String>();
    eventStats.put(HiveHistory.Keys.SESSION_ID.name(), sessionState.getSessionId());
  }

  @Override
  public String getHistFileName() {
    return "HiveQueryCompeletionScribe";
  }

  private void createNewQueryMetricEntry (Map<String, String> eventStats) {
    String QueryID = eventStats.get("QUERY_ID");
    QueryStats newQueryStats;
    newQueryStats = new QueryStats(QueryID, eventStats.get("QUERY_STRING"), System.currentTimeMillis(), 0);
    newQueryStats.plansInfo = new HashMap<String, QueryPlan>();
    newQueryStats.taskProgress = new HashMap<String, String>();
    this.queryStatsMap.put(QueryID, newQueryStats);
  }

  private void scribeQueryMetricEntry (Map<String, String> eventStats) {
    String QueryID = eventStats.get("QUERY_ID");
    QueryStats stats = this.queryStatsMap.get(QueryID);
    stats.queryEnd = System.currentTimeMillis();

    SessionState sessionState = SessionState.get();
    if (sessionState != null && sessionState.getUserName() != null && sessionState.getUserIpAddress() != null && sessionState.getSessionId() != null) {
      stats.username = sessionState.getUserName().toString();
      stats.IPAddress = sessionState.getUserIpAddress().toString();
      stats.sessionID = sessionState.getSessionId().toString();
      stats.database = sessionState.getCurrentDatabase().toString();
      if (sessionState.getMapRedStats() != null) {
        stats.mapReduceStatsDesc = sessionState.getMapRedStats().toString();
      }
      stats.currentTimeStamp = sessionState.getQueryCurrentTimestamp().toString();
      stats.mapReduceStats = sessionState.getMapRedStats();
    }

    QueryCompletedEventScriber hiveHistScriber = new QueryCompletedEventScriber();
    if (stats != null) {
      hiveHistScriber.handle(stats);
      LOG.info("Query stats passed to log pipeline");
      this.queryStatsMap.remove(QueryID);
      LOG.info("Removed Query stats from cache");
    }
    else {
      LOG.info("Stats is null. Nothing passed to log pipeline");
    }
  }

  @Override
  public void startQuery(String cmd, String id) {
    SessionState sessionState = SessionState.get();
    if (sessionState == null) {
      return;
    }
    HiveHistory.QueryInfo queryInfo = new HiveHistory.QueryInfo();
    queryInfo.hm.put(HiveHistory.Keys.QUERY_ID.name(), id);
    queryInfo.hm.put(HiveHistory.Keys.QUERY_STRING.name(), cmd);
    queryInfoMap.put(id, queryInfo);
    createNewQueryMetricEntry(queryInfo.hm);
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
  public void setTaskCounters(String queryId, String taskId, Counters counters) {
    String id = queryId + ":" + taskId;
    HiveHistory.QueryInfo queryInfo = queryInfoMap.get(queryId);
    StringBuilder stringBuilder1 = new StringBuilder("");
    HiveHistory.TaskInfo taskInfo = taskInfoMap.get(id);
    if ((taskInfo == null) || (counters == null)) {
      return;
    }
    StringBuilder stringBuilder = new StringBuilder("");
    try {
      boolean first = true;
      for (Counters.Group group : counters) {
        for (Counters.Counter counter : group) {
          if (first) {
            first = false;
          } else {
            stringBuilder.append(',');
          }
          stringBuilder.append(group.getDisplayName());
          stringBuilder.append('.');
          stringBuilder.append(counter.getDisplayName());
          stringBuilder.append(':');
          stringBuilder.append(counter.getCounter());
          String tab = getRowCountTableName(counter.getDisplayName());
          if (tab != null) {
            if (stringBuilder1.length() > 0) {
              stringBuilder1.append(",");
            }
            stringBuilder1.append(tab);
            stringBuilder1.append('~');
            stringBuilder1.append(counter.getCounter());
            queryInfo.rowCountMap.put(tab, counter.getCounter());
          }
        }
      }
    } catch (Exception e) {
      LOG.warn(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    if (stringBuilder1.length() > 0) {
      taskInfoMap.get(id).hm.put(HiveHistory.Keys.ROWS_INSERTED.name(), stringBuilder1.toString());
      queryInfoMap.get(queryId).hm.put(HiveHistory.Keys.ROWS_INSERTED.name(), stringBuilder1
          .toString());
    }
    if (stringBuilder.length() > 0) {
      taskInfoMap.get(id).hm.put(HiveHistory.Keys.TASK_COUNTERS.name(), stringBuilder.toString());
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
    scribeQueryMetricEntry(queryInfo.hm);
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

    QueryStats stats = queryStatsMap.get(queryId);
    snapshotTaskProgress(stats, taskInfo.hm);
  }

  public void snapshotTaskProgress(QueryStats stats, Map<String, String> taskStats){
    for (Map.Entry<String, String> ent : taskStats.entrySet()) {
      String key = ent.getKey();
      String val = ent.getValue();
      if(val != null) {
        val = val.replace(System.getProperty("line.separator"), " ");
      }
      stats.taskProgress.put(Long.toString(System.currentTimeMillis()), key + "=\"" + val + "\"");
    }
  }

  @Override
  public void endTask(String queryId, Task<? extends Serializable> task) {
    String id = queryId + ":" + task.getId();
    HiveHistory.TaskInfo taskInfo = taskInfoMap.get(id);
    if (taskInfo == null) {
      return;
    }
    QueryStats stats = queryStatsMap.get(queryId);
    snapshotTaskProgress(stats, taskInfo.hm);
    taskInfoMap.remove(id);
  }

  @Override
  public void progressTask(String queryId, Task<? extends Serializable> task) {
    String id = queryId + ":" + task.getId();
    TaskInfo taskInfo = taskInfoMap.get(id);
    if (taskInfo == null) {
      return;
    }
    QueryStats stats = queryStatsMap.get(queryId);
    snapshotTaskProgress(stats, taskInfo.hm);
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
    if (plan == null) {
      return;
    }
    Map<String,String> ctrmap = ctrMapFactory.get();
    ctrmap.put("plan", plan.toString());

    String queryId = plan.getQueryId();
    String id = queryId + ":" + plan.getFetchTask().getId();

    QueryStats stats = queryStatsMap.get(queryId);
    stats.plansInfo.put(Long.toString(System.currentTimeMillis()), plan);
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
