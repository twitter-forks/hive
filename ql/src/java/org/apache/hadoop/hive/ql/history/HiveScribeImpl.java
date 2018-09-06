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

  private QueryCompletedEventScriber hiveHistScriber = new QueryCompletedEventScriber();

  private Map<String, String> idToTableMap = null;

  // Job Hash Map
  private final HashMap<String, QueryInfo> queryInfoMap = new HashMap<>();

  // Task Hash Map
  private final HashMap<String, TaskInfo> taskInfoMap = new HashMap<>();

  private static final String ROW_COUNT_PATTERN = "RECORDS_OUT_(\\d+)(_)*(\\S+)*";

  private static final Pattern rowCountPattern = Pattern.compile(ROW_COUNT_PATTERN);

  // Query Hash Map
  private HashMap<String, QueryStats> queryStatsMap = new HashMap<>();

  /**
   * Construct HiveScribeImpl object and scribe query metric to log pipeline.
   */
  public HiveScribeImpl() {
    LOG.info("Instantiated an instance for HiveScribeImpl to scribe query logs.");
  }

  /**
   * HistFileName is not used in this impl.
   */
  @Override
  public String getHistFileName() {
    return "HiveQueryCompletionScribe";
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

    QueryInfo newQueryInfo = createNewQueryEventEntry(id, cmd);
    queryInfoMap.put(id, newQueryInfo);

    QueryStats newQueryStats = createNewQueryMetricEntry(newQueryInfo.hm, timeStamp);
    queryStatsMap.put(id, newQueryStats);
  }

  /**
   * Create a QueryStats object for each query to store query identity and runtime statistics.
   * Store query id and QueryStats as key-value pairs in queryStatsMap.
   * (Note that queryStatsMap is different from queryInfoMap, where the latter is a scalar event
   * such as query start, query end, task start, etc.)
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

  private QueryInfo createNewQueryEventEntry(String queryId, String queryCommand) {
    QueryInfo queryInfo = new QueryInfo();
    queryInfo.hm.put(Keys.QUERY_ID.name(), queryId);
    queryInfo.hm.put(Keys.QUERY_STRING.name(), queryCommand);
    return queryInfo;
  }

  @Override
  public void setQueryProperty(String queryId, HiveHistory.Keys propName, String propValue) {
    QueryInfo queryInfo = queryInfoMap.get(queryId);
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
    QueryInfo queryInfo = queryInfoMap.get(queryId);
    StringBuilder stringBuilderRowsInserted = new StringBuilder("");
    TaskInfo taskInfo = taskInfoMap.get(id);
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
  }

  private void addSessionInfo(String queryId) {
    QueryStats stats = queryStatsMap.get(queryId);
    SessionState sessionState = SessionState.get();
    if (sessionState == null) {
      return;
    }
    String userName = sessionState.getUserName();
    String ipAddress = sessionState.getUserIpAddress();
    String sessionId = sessionState.getSessionId();
    String currentDatabase = sessionState.getCurrentDatabase();
    String currentTimeStamp = sessionState.getQueryCurrentTimestamp().toString();
    assert (userName != null && ipAddress != null && sessionId != null && currentDatabase != null && currentTimeStamp != null)
        : "Query information is incomplete.";
    stats.setUsername(userName);
    stats.setIPAddress(sessionState.getUserIpAddress());
    stats.setSessionID(sessionState.getSessionId());
    stats.setDatabase(sessionState.getCurrentDatabase());
    stats.setCurrentTimeStamp(sessionState.getQueryCurrentTimestamp().toString());
    stats.setMapReduceStats(sessionState.getMapRedStats());
    if (sessionState.getMapRedStats() != null) {
      stats.setMapReduceStatsDesc(sessionState.getMapRedStats().toString());
    }
  }

  @Override
  public void endQuery(String queryId) {
    Long timeStamp = System.currentTimeMillis();
    QueryStats stats = queryStatsMap.get(queryId);
    stats.setEndTime(timeStamp);
    addSessionInfo(queryId);
    scribeQueryMetricEntry(queryId);
    queryInfoMap.remove(queryId);
    queryStatsMap.remove(queryId);
  }

  @Override
  public void startTask(String queryId, Task<? extends Serializable> task, String taskName) {
    Long timeStamp = System.currentTimeMillis();
    TaskInfo taskInfo = createNewTaskEventEntry(queryId, task.getId(), taskName);
    String id = queryId + ":" + task.getId();
    taskInfoMap.put(id, taskInfo);
    QueryStats stats = queryStatsMap.get(queryId);
    logTaskProgress(RecordTypes.TaskStart, stats, taskInfo.hm, timeStamp);
  }

  private TaskInfo createNewTaskEventEntry(String queryId, String taskId, String taskName) {
    TaskInfo taskInfo = new TaskInfo();
    taskInfo.hm.put(HiveHistory.Keys.QUERY_ID.name(), queryId);
    taskInfo.hm.put(HiveHistory.Keys.TASK_ID.name(), taskId);
    taskInfo.hm.put(HiveHistory.Keys.TASK_NAME.name(), taskName);
    return taskInfo;
  }

  @Override
  public void endTask(String queryId, Task<? extends Serializable> task) {
    Long timeStamp = System.currentTimeMillis();
    String id = queryId + ":" + task.getId();
    TaskInfo taskInfo = taskInfoMap.get(id);
    if (taskInfo == null) {
      return;
    }
    QueryStats stats = queryStatsMap.get(queryId);
    logTaskProgress(RecordTypes.TaskEnd, stats, taskInfo.hm, timeStamp);
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
    logTaskProgress(RecordTypes.TaskProgress, stats, taskInfo.hm, timeStamp);
  }

  private void logTaskProgress(RecordTypes recordTypes, QueryStats stats, Map<String, String> taskStats, Long timeStamp) {
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
    addTaskProgress(stats.getTaskProgress(), timeStamp, snapshot.toString());
  }

  private void addTaskProgress(ArrayList<QueryStats.taskDetail> taskProgress, Long timeStamp, String task) {
    int listSize = taskProgress.size();
    if (listSize > 0) {
      String lastProgress = taskProgress.get(listSize - 1).getProgress();
      if (task.equals(lastProgress)) {
        return;
      }
    }
    QueryStats.taskDetail newTask = new QueryStats.taskDetail();
    newTask.setTimeStamp(timeStamp);
    newTask.setProgress(task);
    taskProgress.add(newTask);
  }

  @Override
  public void logPlanProgress(QueryPlan plan) throws IOException {
    long timeStamp = System.currentTimeMillis();
    if (plan == null) {
      return;
    }
    String queryId = plan.getQueryId();
    QueryStats stats = queryStatsMap.get(queryId);
    addPlan(stats.getPlanProgress(), timeStamp, plan);
  }

  private void addPlan(ArrayList<QueryStats.plan> plansInfo, long timeStamp, QueryPlan plan) {
    int listSize = plansInfo.size();
    if (listSize > 0) {
      String lastProgress = plansInfo.get(listSize - 1).getQueryPlan().toString();
      if (plan.toString().equals(lastProgress)) {
        return;
      }
    }
    QueryStats.plan newPlan = new QueryStats.plan();
    newPlan.setTimeStamp(timeStamp);
    newPlan.setQueryPlan(plan);
    plansInfo.add(newPlan);
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
  private String getRowCountTableName(String name) {
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
