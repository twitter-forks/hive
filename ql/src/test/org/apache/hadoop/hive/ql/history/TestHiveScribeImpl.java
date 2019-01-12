package org.apache.hadoop.hive.ql.history;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.thrift.hive.HiveQueryCompletionEvent;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class TestHiveScribeImpl {
  private static final TDeserializer tDeserializer = new TDeserializer();
  private HiveScribeImpl hiveScriber = null;
  private TwitterScriber twitterScriber = null;
  private SessionState sessionState = null;
  private Map<String, MapRedStats> mapRedStats;
  private MapRedStats oneMapRedStats;
  private QueryPlan plan;
  private String queryId;
  private String queryString;
  private String username;
  private String ipAddress;
  private String sessionId;
  private String dataBase;
  private String planInfoFormat;
  private String scribeCategoryName;
  private String taskName;
  private String taskId;
  private String taskProgress;
  private String taskStart;
  private String taskEnd;
  private String taskInfoFormat;
  private String mapRedKey;
  private Long queryStartTime;
  private Long queryEndTime;
  private Long taskStartTime;
  private Long taskProgressTime;
  private Long taskEndTime;
  private Long planTime;

  @Before
  public void beforeEachTest() {
    hiveScriber = new HiveScribeImpl();
    queryId = "hive_20181011015254_49c74d55-7833-4063-9ba4-a4d8d0271ae0";
    queryString = "describe iesource.client_engagements";
    username = "user";
    ipAddress = "10.53.211.120";
    sessionId = "2aae61d4-374f-4f29-9076-432372003fc2";
    dataBase = "default";
    queryStartTime = Long.parseLong("1539222821986");
    queryEndTime = Long.parseLong("1539223547219");
    scribeCategoryName = "unit_test";

    taskName = "org.apache.hadoop.hive.ql.exec.DDLTask";
    taskId = "Stage-0";
    taskStartTime = Long.parseLong("1539222821986");
    taskProgressTime = Long.parseLong("1539222821986");
    taskEndTime = Long.parseLong("1539222821986");
    taskInfoFormat = String.format("TASK_NAME=\"%s\" QUERY_ID=\"%s\" TASK_ID=\"%s\"", taskName, queryId, taskId);
    taskStart = "TaskStart " + taskInfoFormat;
    taskProgress = "TaskProgress " + taskInfoFormat;
    taskEnd = "TaskEnd " + taskInfoFormat;

    planTime = Long.parseLong("1539222821986");
    planInfoFormat = String.format("PlanInfo(timeStamp:%s, planDetails:PlanDetails(queryId:%s, stageGraph:GraphInfo(), stageList:[], done:[], started:[]))", planTime, queryId);
    plan = new QueryPlan();
    plan.setQueryId(queryId);
    plan.setQuery(new Query());

    oneMapRedStats = new MapRedStats(1, 1, 10, true, "1");
    mapRedStats = new HashMap();
    mapRedKey = "Stage 1";
    mapRedStats.put(mapRedKey, oneMapRedStats);
  }

  @Test
  public void testStartQueryMissingSessionState() {
    hiveScriber.startQuery(queryString, queryId, null, queryStartTime);

    assertTrue(hiveScriber.getQueryStatsMap().keySet().size() == 0);
  }

  @Test
  public void testEntireQueryCycle() throws TException, IOException {
    // Start query
    sessionState = Mockito.mock(SessionState.class);
    when(sessionState.getUserName()).thenReturn(username);
    when(sessionState.getUserIpAddress()).thenReturn(ipAddress);
    when(sessionState.getSessionId()).thenReturn(sessionId);
    when(sessionState.getCurrentDatabase()).thenReturn(dataBase);
    when(sessionState.getMapRedStats()).thenReturn(mapRedStats);

    twitterScriber = Mockito.spy(new TwitterScriber(scribeCategoryName));
    hiveScriber.hiveHistScriber.scriber = twitterScriber;

    hiveScriber.startQuery(queryString, queryId, sessionState, queryStartTime);

    assertTrue(hiveScriber.getQueryStatsMap().keySet().size() == 1);
    assertTrue(hiveScriber.getQueryStatsMap().keySet().contains(queryId));
    assertEquals(queryId, hiveScriber.getQueryStatsMap().get(queryId).getQueryId());
    assertEquals(queryString, hiveScriber.getQueryStatsMap().get(queryId).getQueryString());
    assertEquals((long) queryStartTime, hiveScriber.getQueryStatsMap().get(queryId).getStartTime());

    // Start Task
    hiveScriber.startTask(queryId, taskId, taskName, taskStartTime);
    assertTrue(hiveScriber.getQueryStatsMap().get(queryId).getTaskProgress().size() == 1);
    assertEquals(taskStart, hiveScriber.getQueryStatsMap().get(queryId).getTaskProgress().get(0).getProgress());

    // Progress Task
    hiveScriber.progressTask(queryId, taskId, taskProgressTime);
    assertTrue(hiveScriber.getQueryStatsMap().get(queryId).getTaskProgress().size() == 2);
    assertEquals(taskProgress, hiveScriber.getQueryStatsMap().get(queryId).getTaskProgress().get(1).getProgress());

    // End Task
    hiveScriber.endTask(queryId, taskId, taskEndTime);
    assertTrue(hiveScriber.getQueryStatsMap().get(queryId).getTaskProgress().size() == 3);
    assertEquals(taskEnd, hiveScriber.getQueryStatsMap().get(queryId).getTaskProgress().get(2).getProgress());

    // Progress Plan
    hiveScriber.logPlanProgress(plan, planTime);

    // End query
    hiveScriber.endQuery(queryId, sessionState, queryEndTime);

    // Capture log event at scribe()
    ArgumentCaptor<String> arg = ArgumentCaptor.forClass(String.class);
    Mockito.verify(hiveScriber.hiveHistScriber.scriber).scribe(arg.capture());

    String tMessage = arg.getValue();
    HiveQueryCompletionEvent tEvent = new HiveQueryCompletionEvent();
    tDeserializer.deserialize(tEvent, Base64.getDecoder().decode(tMessage));

    assertEquals(queryId, tEvent.queryId);
    assertEquals(queryString, tEvent.queryString);
    assertEquals(username, tEvent.user);
    assertEquals(ipAddress, tEvent.ip);
    assertEquals(sessionId, tEvent.sessionId);
    assertEquals(dataBase, tEvent.database);
    assertEquals((long) queryEndTime, tEvent.endTime);
    assertEquals((long) queryStartTime, tEvent.startTime);
    assertEquals(mapRedStats.get(mapRedKey).getNumMap(), tEvent.getMapReduceStats().get(mapRedKey).getNumberMappers());
    assertEquals(mapRedStats.get(mapRedKey).getNumReduce(), tEvent.getMapReduceStats().get(mapRedKey).getNumberReducers());
    assertEquals((long) planTime, tEvent.getPlanProgress().get(0).getTimeStamp());
    assertEquals(queryId, tEvent.getPlanProgress().get(0).getPlanDetails().getQueryId());
    assertEquals(planInfoFormat, tEvent.getPlanProgress().get(0).toString());
  }
}
