/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.history;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryStats;
import org.apache.hadoop.hive.ql.plan.api.Adjacency;
import org.apache.hadoop.hive.ql.plan.api.Graph;
import org.apache.hadoop.hive.ql.plan.api.Operator;
import org.apache.hadoop.hive.ql.plan.api.Stage;
import org.apache.hadoop.hive.ql.plan.api.Task;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hive.thriftjava.AdjacencyInfo;
import com.twitter.hive.thriftjava.GraphInfo;
import com.twitter.hive.thriftjava.HiveQueryCompletionEvent;
import com.twitter.hive.thriftjava.OperatorInfo;
import com.twitter.hive.thriftjava.PlanInfo;
import com.twitter.hive.thriftjava.QueryStageInfo;
import com.twitter.hive.thriftjava.StageInfo;
import com.twitter.hive.thriftjava.TaskInfo;
import com.twitter.hive.thriftjava.ProgressInfo;

/**
 * Class that scribes query completion events
 */
public class QueryCompletedEventScriber {

  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.exec.HiveScribeImpl");

  private TwitterScriber scriber = new TwitterScriber("hive_query_completion_event");

  public void handle(QueryStats event) {
    try {
      scriber.scribe(toThriftQueryCompletionEvent(event));
    } catch (TException e) {
      String errorMsg = String.format("Could not serialize thrift object of " +
              "Query(id=%s, user=%s, session=%s, database=%s)",
          event.getQueryID(),
          event.getUsername(),
          event.getSessionID(),
          event.getDatabase());
      LOG.warn(String.format("%s, %s", e, errorMsg));
    }
  }

  private static HiveQueryCompletionEvent toThriftQueryCompletionEvent(QueryStats event) {
    HiveQueryCompletionEvent thriftEvent = new com.twitter.hive.thriftjava.HiveQueryCompletionEvent();
    thriftEvent.queryId = event.getQueryID();
    thriftEvent.queryString = event.getQueryString();
    thriftEvent.startTime = event.getQueryStart();
    thriftEvent.endTime = event.getQueryEnd();
    thriftEvent.user = event.getUsername();
    thriftEvent.ip = event.getIPAddress();
    thriftEvent.sessionId = event.getSessionID();
    thriftEvent.database = event.getDatabase();
    thriftEvent.plansInfo = new HashMap<String, PlanInfo>();
    thriftEvent.taskProgress = new ArrayList<ProgressInfo>();
    thriftEvent.mapReduceInfo = new HashMap<String, QueryStageInfo>();

    setPlansInfo(thriftEvent.plansInfo, event.getPlansInfo());
    setTaskProgress(thriftEvent.taskProgress, event.getTaskProgress());
    setMapReduceStats(thriftEvent.mapReduceInfo, event.getMapReduceStats());

    return thriftEvent;
  }

  /**
   * Update plansInfo for thrift object according to pre-defined schema
   */
  private static void setPlansInfo(Map<String, PlanInfo> thriftPlansInfo, Map<String, QueryPlan> plansInfo) {
    if (plansInfo == null) {
      return;
    }
    for (Map.Entry<String, QueryPlan> ent : plansInfo.entrySet()) {
      PlanInfo thriftPlanInfo = new com.twitter.hive.thriftjava.PlanInfo();
      String key = ent.getKey();
      thriftPlanInfo.queryId = ent.getValue().getQueryId();

      thriftPlanInfo.queryType = ent.getValue().getQuery().getQueryType();
      thriftPlanInfo.done = ent.getValue().getDone().toString();
      thriftPlanInfo.started = ent.getValue().getStarted().toString();

      thriftPlanInfo.queryAttributes = new HashMap<String, String>();
      setMapValForString(thriftPlanInfo.queryAttributes, ent.getValue().getQuery().getQueryAttributes());

      thriftPlanInfo.queryCounters = new HashMap<String, Long>();
      setMapValForLong(thriftPlanInfo.queryCounters, ent.getValue().getQuery().getQueryCounters());

      thriftPlanInfo.stageGraph = new GraphInfo();
      setStageGraph(thriftPlanInfo.stageGraph, ent.getValue().getQuery().getStageGraph());

      thriftPlanInfo.stageList = new ArrayList<StageInfo>();
      setStageList(thriftPlanInfo.stageList, ent.getValue().getQuery().getStageList());

      thriftPlansInfo.put(key, thriftPlanInfo);
    }
  }

  private static void setStageGraph(GraphInfo thriftStageGraph, Graph stageGraph) {
    if (stageGraph == null) {
      return;
    }
    thriftStageGraph.nodeType = stageGraph.getNodeType().toString();
    setListString(thriftStageGraph.roots, stageGraph.getRoots());
    thriftStageGraph.adjacencyList = new ArrayList<AdjacencyInfo>();

    for (int i = 0; i < stageGraph.getAdjacencyListSize(); i++) {
      AdjacencyInfo adjacencyListEnt = new AdjacencyInfo();
      adjacencyListEnt.node = stageGraph.getAdjacencyList().get(i).getNode();
      adjacencyListEnt.children = stageGraph.getAdjacencyList().get(i).getChildren();
      adjacencyListEnt.adjacencyType = stageGraph.getAdjacencyList().get(i).getAdjacencyType().toString();
      thriftStageGraph.adjacencyList.add(adjacencyListEnt);
    }
  }

  private static void setMapReduceStats(Map<String, QueryStageInfo> thriftMapReduceInfo, Map<String, MapRedStats> mapReduceInfo) {
    if (mapReduceInfo == null) {
      return;
    }

    for (Map.Entry<String, MapRedStats> ent : mapReduceInfo.entrySet()) {
      QueryStageInfo thriftCounterInfo = new com.twitter.hive.thriftjava.QueryStageInfo();
      String key = ent.getKey();
      thriftCounterInfo.stageId = key;
      thriftCounterInfo.jobId = ent.getValue().getJobId();
      thriftCounterInfo.cpuMsec = ent.getValue().getCpuMSec();
      thriftCounterInfo.counters = ent.getValue().getCounters().toString();
      thriftCounterInfo.numberMappers = ent.getValue().getNumMap();
      thriftCounterInfo.numberReducers = ent.getValue().getNumReduce();
      thriftCounterInfo.taskNumbers = ent.getValue().getTaskNumbers().toString();
      thriftMapReduceInfo.put(key, thriftCounterInfo);
    }
  }

  private static void setStageList (List<StageInfo> thriftStageList, List<Stage> stageList) {
    if (stageList == null) {
      return;
    }
    for (int i = 0; i < stageList.size(); i++) {
      StageInfo stageEnt = new StageInfo();
      stageEnt.stageId = stageList.get(i).getStageId();
      stageEnt.stageType = stageList.get(i).getStageType().toString();

      stageEnt.stageAttributes = new HashMap<String,String>();
      setMapValForString(stageEnt.stageAttributes, stageList.get(i).getStageAttributes());

      stageEnt.stageCounters = new HashMap<String,Long>();
      setMapValForLong(stageEnt.stageCounters, stageList.get(i).getStageCounters());

      stageEnt.taskList = new ArrayList<TaskInfo>();
      setTaskList(stageEnt.taskList, stageList.get(i).getTaskList());

      stageEnt.done = stageList.get(i).isDone();
      stageEnt.started = stageList.get(i).isStarted();
      thriftStageList.add(stageEnt);
    }
  }

  private static void setTaskList(List<TaskInfo> thriftTaskList, List<Task> taskList) {
    if (taskList == null) {
      return;
    }
    for (int i = 0; i < taskList.size(); i++) {
      TaskInfo thriftTaskListEnt = new TaskInfo();
      thriftTaskListEnt.taskId = taskList.get(i).getTaskId();
      thriftTaskListEnt.taskType = taskList.get(i).getTaskType().toString();
      thriftTaskListEnt.done = taskList.get(i).isDone();
      thriftTaskListEnt.started = taskList.get(i).isStarted();

      thriftTaskListEnt.taskAttributes = new HashMap<String, String>();
      setMapValForString(thriftTaskListEnt.taskAttributes, taskList.get(i).getTaskAttributes());

      thriftTaskListEnt.taskCounters = new HashMap<String, Long>();
      setMapValForLong(thriftTaskListEnt.taskCounters, taskList.get(i).getTaskCounters());

      thriftTaskListEnt.operatorGraph = new GraphInfo();
      setOperatorGraph(thriftTaskListEnt.operatorGraph, taskList.get(i).getOperatorGraph());

      thriftTaskListEnt.operatorList = new ArrayList<OperatorInfo>();
      setOperatorList(thriftTaskListEnt.operatorList, taskList.get(i).getOperatorList());

      thriftTaskList.add(thriftTaskListEnt);
    }
  }

  public static void setListString(List<String> thriftStringList, List<String> stringList) {
    if (stringList == null) {
      return;
    }
    for (int i = 0; i < stringList.size(); i++) {
      thriftStringList.add(stringList.get(i));
    }
  }

  public static void setTaskProgress(List<ProgressInfo> thriftTaskProgress, ArrayList<QueryStats.progressSnapshot> taskProgress) {
    if (taskProgress == null) {
      return;
    }
    for (int i = 0; i < taskProgress.size(); i++) {
      ProgressInfo thriftProgressInfo = new com.twitter.hive.thriftjava.ProgressInfo();
      thriftProgressInfo.timeStamp = taskProgress.get(i).getTimeStamp();
      thriftProgressInfo.value = taskProgress.get(i).getValue();
      thriftTaskProgress.add(thriftProgressInfo);
    }
  }

  public static void setMapValForString(Map<String, String> thriftConfigs, Map<String, String> configs) {
    if (configs == null) {
      return;
    }
    for (Map.Entry<String, String> ent : configs.entrySet()) {
      String key = ent.getKey();
      String val = ent.getValue();
      thriftConfigs.put(key, val);
    }
  }

  public static void setMapValForLong(Map<String, Long> thriftConfigs, Map<String, Long> configs) {
    if (configs == null) {
      return;
    }
    for (Map.Entry<String, Long> ent : configs.entrySet()) {
      String key = ent.getKey();
      Long val = ent.getValue();
      thriftConfigs.put(key, val);
    }
  }

  private static void setOperatorGraph(GraphInfo thriftOperatorGraph, Graph operatorGraph) {
    if (operatorGraph == null) {
      return;
    }
    thriftOperatorGraph.nodeType = operatorGraph.getNodeType().toString();
    setListString(thriftOperatorGraph.roots, operatorGraph.getRoots());

    thriftOperatorGraph.adjacencyList = new ArrayList<AdjacencyInfo>();
    setAdjacencyList(thriftOperatorGraph.adjacencyList, operatorGraph.getAdjacencyList());
  }

  private static void setAdjacencyList(List<AdjacencyInfo>thriftAdjacencyList, List<Adjacency> adjacencyList) {
    if (adjacencyList == null) {
      return;
    }
    for (int i = 0; i < adjacencyList.size(); i++) {
      AdjacencyInfo adjacencyInfo = new AdjacencyInfo();
      adjacencyInfo.node = adjacencyList.get(i).getNode();
      adjacencyInfo.children = adjacencyList.get(i).getChildren();
      adjacencyInfo.adjacencyType = adjacencyList.get(i).getAdjacencyType().toString();
      thriftAdjacencyList.add(adjacencyInfo);
    }
  }

  private static void setOperatorList(List<OperatorInfo> thriftOperatorList, List<Operator> operatorList) {
    if (operatorList == null) {
      return;
    }
    for (int i = 0; i < operatorList.size(); i++) {
      OperatorInfo operatorInfo = new OperatorInfo();
      operatorInfo.operatorId = operatorList.get(i).getOperatorId();
      operatorInfo.operatorType = operatorList.get(i).getOperatorType().toString();
      operatorInfo.done = operatorList.get(i).isDone();
      operatorInfo.started = operatorList.get(i).isStarted();
      operatorInfo.operatorAttributes = new HashMap<String,String>();
      setMapValForString(operatorInfo.operatorAttributes, operatorList.get(i).getOperatorAttributes());
      operatorInfo.operatorCounters = new HashMap<String,Long>();
      setMapValForLong(operatorInfo.operatorCounters, operatorList.get(i).getOperatorCounters());
      thriftOperatorList.add(operatorInfo);
    }
  }
}
