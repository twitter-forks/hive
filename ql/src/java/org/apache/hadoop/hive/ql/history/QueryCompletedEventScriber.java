/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.history;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    HiveQueryCompletionEvent thriftEvent = new HiveQueryCompletionEvent();
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
      PlanInfo thriftPlanInfo = new PlanInfo();
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
    thriftStageGraph.roots = new ArrayList<String>();
    if (stageGraph.getRoots() != null) {
      thriftStageGraph.roots.addAll(stageGraph.getRoots());
    }
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
      QueryStageInfo thriftCounterInfo = new QueryStageInfo();
      String key = ent.getKey();
      thriftCounterInfo.stageId = key;
      thriftCounterInfo.jobId = ent.getValue().getJobId();
      thriftCounterInfo.cpuMsec = ent.getValue().getCpuMSec();
      thriftCounterInfo.counters = ent.getValue().getCounters().toString();
      thriftCounterInfo.numberMappers = ent.getValue().getNumMap();
      thriftCounterInfo.numberReducers = ent.getValue().getNumReduce();
      thriftCounterInfo.taskNumbers = ent.getValue().getTaskNumbers();
      thriftMapReduceInfo.put(key, thriftCounterInfo);
    }
  }

  private static void setStageList(List<StageInfo> thriftStageList, List<Stage> stageList) {
    if (stageList == null) {
      return;
    }
    for (Stage aStage : stageList) {
      StageInfo stageEnt = new StageInfo();
      stageEnt.stageId = aStage.getStageId();
      stageEnt.stageType = aStage.getStageType().toString();

      stageEnt.stageAttributes = new HashMap<String, String>();
      setMapValForString(stageEnt.stageAttributes, aStage.getStageAttributes());

      stageEnt.stageCounters = new HashMap<String, Long>();
      setMapValForLong(stageEnt.stageCounters, aStage.getStageCounters());

      stageEnt.taskList = new ArrayList<TaskInfo>();
      setTaskList(stageEnt.taskList, aStage.getTaskList());

      stageEnt.done = aStage.isDone();
      stageEnt.started = aStage.isStarted();
      thriftStageList.add(stageEnt);
    }
  }

  private static void setTaskList(List<TaskInfo> thriftTaskList, List<Task> taskList) {
    if (taskList == null) {
      return;
    }
    for (Task aTask : taskList) {
      TaskInfo thriftTaskListEnt = new TaskInfo();
      thriftTaskListEnt.taskId = aTask.getTaskId();
      thriftTaskListEnt.taskType = aTask.getTaskType().toString();
      thriftTaskListEnt.done = aTask.isDone();
      thriftTaskListEnt.started = aTask.isStarted();

      thriftTaskListEnt.taskAttributes = new HashMap<String, String>();
      setMapValForString(thriftTaskListEnt.taskAttributes, aTask.getTaskAttributes());

      thriftTaskListEnt.taskCounters = new HashMap<String, Long>();
      setMapValForLong(thriftTaskListEnt.taskCounters, aTask.getTaskCounters());

      thriftTaskListEnt.operatorGraph = new GraphInfo();
      setOperatorGraph(thriftTaskListEnt.operatorGraph, aTask.getOperatorGraph());

      thriftTaskListEnt.operatorList = new ArrayList<OperatorInfo>();
      setOperatorList(thriftTaskListEnt.operatorList, aTask.getOperatorList());

      thriftTaskList.add(thriftTaskListEnt);
    }
  }

  private static void setTaskProgress(List<ProgressInfo> thriftTaskProgress, ArrayList<QueryStats.progressSnapshot> taskProgress) {
    if (taskProgress == null) {
      return;
    }
    for (QueryStats.progressSnapshot progress : taskProgress) {
      ProgressInfo thriftProgressInfo = new ProgressInfo();
      thriftProgressInfo.timeStamp = progress.getTimeStamp();
      thriftProgressInfo.value = progress.getProgress();
      thriftTaskProgress.add(thriftProgressInfo);
    }
  }

  private static void setMapValForString(Map<String, String> thriftConfigs, Map<String, String> configs) {
    if (configs == null) {
      return;
    }
    for (Map.Entry<String, String> ent : configs.entrySet()) {
      String key = ent.getKey();
      String val = ent.getValue();
      thriftConfigs.put(key, val);
    }
  }

  private static void setMapValForLong(Map<String, Long> thriftConfigs, Map<String, Long> configs) {
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
    thriftOperatorGraph.roots = new ArrayList<String>();
    if (operatorGraph.getRoots() != null) {
      thriftOperatorGraph.roots.addAll(operatorGraph.getRoots());
    }
    thriftOperatorGraph.adjacencyList = new ArrayList<AdjacencyInfo>();
    setAdjacencyList(thriftOperatorGraph.adjacencyList, operatorGraph.getAdjacencyList());
  }

  private static void setAdjacencyList(List<AdjacencyInfo> thriftAdjacencyList, List<Adjacency> adjacencyList) {
    if (adjacencyList == null) {
      return;
    }
    for (Adjacency anAdjacency : adjacencyList) {
      AdjacencyInfo adjacencyInfo = new AdjacencyInfo();
      adjacencyInfo.node = anAdjacency.getNode();
      adjacencyInfo.children = anAdjacency.getChildren();
      adjacencyInfo.adjacencyType = anAdjacency.getAdjacencyType().toString();
      thriftAdjacencyList.add(adjacencyInfo);
    }
  }

  private static void setOperatorList(List<OperatorInfo> thriftOperatorList, List<Operator> operatorList) {
    if (operatorList == null) {
      return;
    }
    for (Operator anOperator : operatorList) {
      OperatorInfo operatorInfo = new OperatorInfo();
      operatorInfo.operatorId = anOperator.getOperatorId();
      operatorInfo.operatorType = anOperator.getOperatorType().toString();
      operatorInfo.done = anOperator.isDone();
      operatorInfo.started = anOperator.isStarted();
      operatorInfo.operatorAttributes = new HashMap<String, String>();
      setMapValForString(operatorInfo.operatorAttributes, anOperator.getOperatorAttributes());
      operatorInfo.operatorCounters = new HashMap<String, Long>();
      setMapValForLong(operatorInfo.operatorCounters, anOperator.getOperatorCounters());
      thriftOperatorList.add(operatorInfo);
    }
  }
}
