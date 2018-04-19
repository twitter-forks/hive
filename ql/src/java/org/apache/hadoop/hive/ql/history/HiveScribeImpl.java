package org.apache.hadoop.hive.ql.history;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.MapRedStats;
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

  private static final Random randGen = new Random();

  private SessionState.LogHelper console;

  private Map<String, String> idToTableMap = null;

  // Job Hash Map
  private final HashMap<String, HiveHistory.QueryInfo> queryInfoMap = new HashMap<String, HiveHistory.QueryInfo>();

  // Task Hash Map
  private final HashMap<String, HiveHistory.TaskInfo> taskInfoMap = new HashMap<String, HiveHistory.TaskInfo>();

  private static final String DELIMITER = " ";

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
    log(HiveHistory.RecordTypes.SessionStart, hm);
  }

  @Override
  public String getHistFileName() {
    histFileName = "To be decided";
    return String.format("%s", histFileName);
  }

  /**
   * Write the a history record to history file.
   *
   * @param rt
   * @param keyValMap
   */
  void log(HiveHistory.RecordTypes rt, Map<String, String> keyValMap) {
    String QueryID;
    QueryStats newQueryStats;
    QueryStats stats;

    if (rt.toString() == "QueryStart") {
      QueryID = keyValMap.get("QUERY_ID");
      newQueryStats = new QueryStats(QueryID, keyValMap.get("QUERY_STRING"), System.currentTimeMillis(), 0);
      this.queryStatsMap.put(QueryID, newQueryStats);
    }
    else if (rt.toString() == "QueryEnd") {
      QueryID = keyValMap.get("QUERY_ID");
      stats = this.queryStatsMap.get(QueryID);
      stats.queryEnd = System.currentTimeMillis();
      String msg = String.format("QueryID: %s\nStart time: %s\nEnd time: %s\nQuery string: %s\n",
          QueryID, stats.queryStart, stats.queryEnd, stats.queryString);

      // Append session info from SessionState ss = SessionState.get();
      SessionState ss = SessionState.get();
      String sessionInfo = "Missing session info\n";
      String MapReduceInfo = "";
      if (ss != null && ss.getUserName() != null && ss.getUserIpAddress() != null && ss.getSessionId() != null) {
        sessionInfo = String.format("Usename: %s\nIP address: %s\nSessionID: %s\nCurrent database: %s\nCurrent timestamp: \nMap-Reduce Stats:%s\n",
            ss.getUserName().toString(), ss.getUserIpAddress().toString(), ss.getSessionId().toString(),
            ss.getCurrentDatabase().toString(), ss.getQueryCurrentTimestamp().toString(),
            ss.getMapRedStats().toString());

        stats.username = ss.getUserName().toString();
        stats.IPAddress = ss.getUserIpAddress().toString();
        stats.sessionID = ss.getSessionId().toString();
        stats.database = ss.getCurrentDatabase().toString();
        stats.mapReduceStatsDesc = ss.getMapRedStats().toString();
        stats.currentTimeStamp = ss.getQueryCurrentTimestamp().toString();
        stats.mapReduceStats = ss.getMapRedStats();

        for (Map.Entry<String, MapRedStats> ent : ss.getMapRedStats().entrySet()) {
          MapReduceInfo += String.format("Stage ID: %s\nJob ID: %s\nCPU MSec: %s\nCounters: %s\nNumMap: %s\nNumReduce: %s\nTask numbers: %s\n",
              ent.getKey(), ent.getValue().getJobId(), ent.getValue().getCpuMSec(), ent.getValue().getCounters().toString(),
              ent.getValue().getNumMap(), ent.getValue().getNumReduce(), ent.getValue().getTaskNumbers());
        }
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
  }

  @Override
  public void startQuery(String cmd, String id) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      return;
    }
    HiveHistory.QueryInfo ji = new HiveHistory.QueryInfo();

    ji.hm.put(HiveHistory.Keys.QUERY_ID.name(), id);
    ji.hm.put(HiveHistory.Keys.QUERY_STRING.name(), cmd);

    queryInfoMap.put(id, ji);

    log(HiveHistory.RecordTypes.QueryStart, ji.hm);
  }


  @Override
  public void setQueryProperty(String queryId, HiveHistory.Keys propName, String propValue) {
    HiveHistory.QueryInfo ji = queryInfoMap.get(queryId);
    if (ji == null) {
      return;
    }
    ji.hm.put(propName.name(), propValue);
  }

  @Override
  public void setTaskProperty(String queryId, String taskId, HiveHistory.Keys propName,
                              String propValue) {
    String id = queryId + ":" + taskId;
    HiveHistory.TaskInfo ti = taskInfoMap.get(id);
    if (ti == null) {
      return;
    }
    ti.hm.put(propName.name(), propValue);
  }

  @Override
  public void setTaskCounters(String queryId, String taskId, Counters ctrs) {
    String id = queryId + ":" + taskId;
    HiveHistory.QueryInfo ji = queryInfoMap.get(queryId);
    StringBuilder sb1 = new StringBuilder("");
    HiveHistory.TaskInfo ti = taskInfoMap.get(id);
    if ((ti == null) || (ctrs == null)) {
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
            ji.rowCountMap.put(tab, counter.getCounter());

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
    HiveHistory.QueryInfo ji = queryInfoMap.get(queryId);
    if (ji == null) {
      return;
    }
    for (String tab : ji.rowCountMap.keySet()) {
      console.printInfo(ji.rowCountMap.get(tab) + " Rows loaded to " + tab);
    }
  }

  @Override
  public void endQuery(String queryId) {
    HiveHistory.QueryInfo ji = queryInfoMap.get(queryId);
    if (ji == null) {
      return;
    }
    log(HiveHistory.RecordTypes.QueryEnd, ji.hm);
    queryInfoMap.remove(queryId);
  }

  @Override
  public void startTask(String queryId, Task<? extends Serializable> task,
                        String taskName) {
    HiveHistory.TaskInfo ti = new HiveHistory.TaskInfo();

    ti.hm.put(HiveHistory.Keys.QUERY_ID.name(), queryId);
    ti.hm.put(HiveHistory.Keys.TASK_ID.name(), task.getId());
    ti.hm.put(HiveHistory.Keys.TASK_NAME.name(), taskName);

    String id = queryId + ":" + task.getId();
    taskInfoMap.put(id, ti);

    log(HiveHistory.RecordTypes.TaskStart, ti.hm);
  }

  @Override
  public void endTask(String queryId, Task<? extends Serializable> task) {
    String id = queryId + ":" + task.getId();
    HiveHistory.TaskInfo ti = taskInfoMap.get(id);

    if (ti == null) {
      return;
    }
    log(HiveHistory.RecordTypes.TaskEnd, ti.hm);
    taskInfoMap.remove(id);
  }

  @Override
  public void progressTask(String queryId, Task<? extends Serializable> task) {
    String id = queryId + ":" + task.getId();
    HiveHistory.TaskInfo ti = taskInfoMap.get(id);
    if (ti == null) {
      return;
    }
    log(HiveHistory.RecordTypes.TaskProgress, ti.hm);
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
      log(HiveHistory.RecordTypes.Counters, ctrmap);
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
    //IOUtils.closeStream(histStream);
  }

  @Override
  public void finalize() throws Throwable {
    closeStream();
    super.finalize();
  }
}
