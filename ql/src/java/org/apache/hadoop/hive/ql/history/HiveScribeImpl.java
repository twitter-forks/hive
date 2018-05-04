package org.apache.hadoop.hive.ql.history;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
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
  private final HashMap<String, QueryInfo> queryInfoMap = new HashMap<>();

  // Task Hash Map
  private final HashMap<String, TaskInfo> taskInfoMap = new HashMap<String, TaskInfo>();

  private static final String ROW_COUNT_PATTERN = "RECORDS_OUT_(\\d+)(_)*(\\S+)*";

  private static final Pattern rowCountPattern = Pattern.compile(ROW_COUNT_PATTERN);

  // Query Hash Map
  private HashMap<String, QueryStats> queryStatsMap = new HashMap<>();

  /**
   * Construct HiveScribeImpl object and scribe query metric to log pipeline.
   */
  public HiveScribeImpl(SessionState sessionState) {
    LOG.info("Instantiated an instance for HiveScribeImpl to scribe query logs.");
  }

  /**
   * HistFileName is not used in this impl.
   */
  @Override
  public String getHistFileName() {
    return "HiveQueryCompeletionScribe";
  }

  /**
   * Call TwitterScriber to scribe hive query statistics to log pipeline
   */
  private void scribeQueryMetricEntry(String queryID) {
    QueryStats stats = queryStatsMap.get(queryID);
    if (stats == null) {
      LOG.info("Stats is null. Nothing passed to log pipeline");
      return;
    }
    QueryCompletedEventScriber hiveHistScriber = new QueryCompletedEventScriber();
    hiveHistScriber.handle(stats);
    LOG.info("Query stats passed to log pipeline");
  }

  @Override
  public void startQuery(String cmd, String id) {
    Long timeStamp = System.currentTimeMillis();
    SessionState sessionState = SessionState.get();
    if (sessionState == null) {
      return;
    }

    HiveHistory.QueryInfo queryInfo = new HiveHistory.QueryInfo();
    queryInfo.hm.put(Keys.QUERY_ID.name(), id);
    queryInfo.hm.put(Keys.QUERY_STRING.name(), cmd);
    queryInfoMap.put(id, queryInfo);

    QueryStats newQueryStats = createNewQueryMetricEntry(queryInfo.hm, timeStamp);
    queryStatsMap.put(id, newQueryStats);
  }

  /**
   * Create a QueryStats object for each query to store query info and runtime statistics.
   * Store query id and QueryStats as key-value pairs in queryStatsMap.
   * Statistics for each query is updated as tasks and plans progress, and are passed to scriber
   * after each query is completed. Scribed entry will be removed from queryStatsMap in the end.
   *
   * @return QueryStats
   */
  private QueryStats createNewQueryMetricEntry(Map<String, String> eventStats, Long timeStamp) {
    String QueryID = eventStats.get(Keys.QUERY_ID.name());
    String QueryString = eventStats.get(Keys.QUERY_STRING.name());
    return new QueryStats(QueryID, QueryString, timeStamp);
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
    StringBuilder stringBuilderRowsInserted = new StringBuilder("");
    HiveHistory.TaskInfo taskInfo = taskInfoMap.get(id);
    if ((taskInfo == null) || (counters == null)) {
      return;
    }
    StringBuilder stringBuilderTaskCounters = new StringBuilder("");
    try {
      boolean first = true;
      for (Counters.Group group : counters) {
        for (Counters.Counter counter : group) {
          if (first) {
            first = false;
          } else {
            stringBuilderTaskCounters.append(',');
          }
          stringBuilderTaskCounters.append(group.getDisplayName());
          stringBuilderTaskCounters.append('.');
          stringBuilderTaskCounters.append(counter.getDisplayName());
          stringBuilderTaskCounters.append(':');
          stringBuilderTaskCounters.append(counter.getCounter());
          String tab = getRowCountTableName(counter.getDisplayName());
          if (tab != null) {
            if (stringBuilderRowsInserted.length() > 0) {
              stringBuilderRowsInserted.append(",");
            }
            stringBuilderRowsInserted.append(tab);
            stringBuilderRowsInserted.append('~');
            stringBuilderRowsInserted.append(counter.getCounter());
            queryInfo.rowCountMap.put(tab, counter.getCounter());
          }
        }
      }
    } catch (Exception e) {
      LOG.warn(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    if (stringBuilderRowsInserted.length() > 0) {
      taskInfoMap.get(id).hm.put(HiveHistory.Keys.ROWS_INSERTED.name(), stringBuilderRowsInserted.toString());
      queryInfoMap.get(queryId).hm.put(HiveHistory.Keys.ROWS_INSERTED.name(), stringBuilderRowsInserted
          .toString());
    }
    if (stringBuilderTaskCounters.length() > 0) {
      taskInfoMap.get(id).hm.put(HiveHistory.Keys.TASK_COUNTERS.name(), stringBuilderTaskCounters.toString());
    }
  }

  public HashMap<String, QueryStats> getQueryStatsMap() {
    return queryStatsMap;
  }

  @Override
  public void printRowCount(String queryId) {
    return;
  }

  private void addSessionInfo(String queryId) {
    QueryStats stats = queryStatsMap.get(queryId);
    SessionState sessionState = SessionState.get();

    if (sessionState != null && sessionState.getUserName() != null
        && sessionState.getUserIpAddress() != null && sessionState.getSessionId() != null) {
      stats.setUsername(sessionState.getUserName());
      stats.setIPAddress(sessionState.getUserIpAddress());
      stats.setSessionID(sessionState.getSessionId());
      stats.setDatabase(sessionState.getCurrentDatabase());
      if (sessionState.getMapRedStats() != null) {
        stats.setMapReduceStatsDesc(sessionState.getMapRedStats().toString());
      }
      stats.setCurrentTimeStamp(sessionState.getQueryCurrentTimestamp().toString());
      stats.setMapReduceStats(sessionState.getMapRedStats());
    }
  }

  @Override
  public void endQuery(String queryId) {
    Long timeStamp = System.currentTimeMillis();
    QueryStats stats = queryStatsMap.get(queryId);
    stats.setQueryEnd(timeStamp);
    addSessionInfo(queryId);
    scribeQueryMetricEntry(queryId);
    queryInfoMap.remove(queryId);
    queryStatsMap.remove(queryId);
  }

  @Override
  public void startTask(String queryId, Task<? extends Serializable> task, String taskName) {
    Long timeStamp = System.currentTimeMillis();
    HiveHistory.TaskInfo taskInfo = new HiveHistory.TaskInfo();
    taskInfo.hm.put(HiveHistory.Keys.QUERY_ID.name(), queryId);
    taskInfo.hm.put(HiveHistory.Keys.TASK_ID.name(), task.getId());
    taskInfo.hm.put(HiveHistory.Keys.TASK_NAME.name(), taskName);
    String id = queryId + ":" + task.getId();
    taskInfoMap.put(id, taskInfo);
    QueryStats stats = queryStatsMap.get(queryId);
    snapshotTaskProgress(RecordTypes.TaskStart, stats, taskInfo.hm, timeStamp);
  }

  @Override
  public void endTask(String queryId, Task<? extends Serializable> task) {
    Long timeStamp = System.currentTimeMillis();
    String id = queryId + ":" + task.getId();
    HiveHistory.TaskInfo taskInfo = taskInfoMap.get(id);
    if (taskInfo == null) {
      return;
    }

    QueryStats stats = queryStatsMap.get(queryId);
    snapshotTaskProgress(RecordTypes.TaskEnd, stats, taskInfo.hm, timeStamp);
    taskInfoMap.remove(id);
  }

  @Override
  public void progressTask(String queryId, Task<? extends Serializable> task) {
    Long timeStamp = System.currentTimeMillis();
    String id = queryId + ":" + task.getId();
    TaskInfo taskInfo = taskInfoMap.get(id);
    if (taskInfo == null) {
      return;
    }

    QueryStats stats = queryStatsMap.get(queryId);
    snapshotTaskProgress(RecordTypes.TaskProgress, stats, taskInfo.hm, timeStamp);
  }

  public void snapshotTaskProgress(RecordTypes recordTypes, QueryStats stats, Map<String, String> taskStats, Long timeStamp) {
    StringBuilder snapshot = new StringBuilder("");
    snapshot.append(recordTypes.name());
    for (Map.Entry<String, String> ent : taskStats.entrySet()) {
      String key = ent.getKey();
      String val = ent.getValue();
      if (val != null) {
        val = val.replace(System.getProperty("line.separator"), " ");
      }
      snapshot.append(" ");
      snapshot.append(key + "=\"" + val + "\"");
    }
    insertTaskProgress(stats.getTaskProgress(), timeStamp, snapshot.toString());
  }

  private void insertTaskProgress(ArrayList<QueryStats.progressSnapshot> taskProgressStats, Long timeStamp, String taskProgress) {
    int listSize = taskProgressStats.size();
    String lastProgress = taskProgressStats.get(listSize - 1).getProgress();
    if (listSize > 0 && taskProgress.equals(lastProgress)) {
      return;
    }
    QueryStats.progressSnapshot newSnapshot = new QueryStats.progressSnapshot();
    newSnapshot.setTimeStamp(timeStamp);
    newSnapshot.setProgress(taskProgress);
    taskProgressStats.add(newSnapshot);
  }

  @Override
  public void logPlanProgress(QueryPlan plan) throws IOException {
    String timeStamp = Long.toString(System.currentTimeMillis());
    if (plan == null) {
      return;
    }

    String queryId = plan.getQueryId();
    QueryStats stats = queryStatsMap.get(queryId);
    insertPlan(stats.getPlansInfo(), timeStamp, plan);
  }

  private void insertPlan(Map<String, QueryPlan> plansInfo, String timeStamp, QueryPlan plan) {
    for (Map.Entry<String, QueryPlan> ent : plansInfo.entrySet()) {
      if (plan.toString().equals(ent.getValue().toString())) {
        return;
      }
    }
    plansInfo.put(timeStamp, plan);
  }

  @Override
  public void setIdToTableMap(Map<String, String> map) {
    idToTableMap = map;
  }

  /**
   * Returns table name for the counter name.
   *
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
  }

  @Override
  public void finalize() throws Throwable {
    closeStream();
    super.finalize();
  }
}
