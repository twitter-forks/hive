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
import com.twitter.hive.thriftjava.TaskDetailInfo;
import com.twitter.hive.thriftjava.PlanDetails;

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
          event.getQueryId(),
          event.getUsername(),
          event.getSessionID(),
          event.getDatabase());
      LOG.warn(String.format("%s, %s", e, errorMsg));
    }
  }

  private static HiveQueryCompletionEvent toThriftQueryCompletionEvent(QueryStats event) {
    HiveQueryCompletionEvent thriftEvent = new HiveQueryCompletionEvent();
    thriftEvent.queryId = event.getQueryId();
    thriftEvent.queryString = event.getQueryString();
    thriftEvent.startTime = event.getStartTime();
    thriftEvent.endTime = event.getEndTime();
    thriftEvent.user = event.getUsername();
    thriftEvent.ip = event.getIPAddress();
    thriftEvent.sessionId = event.getSessionID();
    thriftEvent.database = event.getDatabase();
    thriftEvent.planProgress = new ArrayList<PlanInfo>();
    thriftEvent.taskProgress = new ArrayList<TaskDetailInfo>();
    thriftEvent.mapReduceStats = new HashMap<String, QueryStageInfo>();

    setPlansInfo(thriftEvent.planProgress, event.getPlanProgress());
    setTaskProgress(thriftEvent.taskProgress, event.getTaskProgress());
    setMapReduceStats(thriftEvent.mapReduceStats, event.getMapReduceStats());

    return thriftEvent;
  }

  /**
   * Update plansInfo for thrift object according to pre-defined schema
   */
  private static void setPlansInfo(List<PlanInfo> thriftPlansInfo, ArrayList<QueryStats.plan> planProgress) {
    if (planProgress == null) {
      return;
    }
    for (QueryStats.plan planEnt : planProgress) {
      PlanInfo thriftPlanInfo = new PlanInfo();
      thriftPlanInfo.timeStamp = planEnt.getTimeStamp();
      thriftPlanInfo.planDetails = new PlanDetails();
      setPlanDetails(thriftPlanInfo.planDetails, planEnt.getQueryPlan());
      thriftPlansInfo.add(thriftPlanInfo);
    }
  }

  private static void setPlanDetails(PlanDetails thriftPlanDetails, QueryPlan plan) {
    thriftPlanDetails.queryId = plan.getQueryId();

    thriftPlanDetails.queryType = plan.getQuery().getQueryType();
    thriftPlanDetails.done = plan.getDone().toString();
    thriftPlanDetails.started = plan.getStarted().toString();

    thriftPlanDetails.queryAttributes = new HashMap<String, String>();
    setMapValForString(thriftPlanDetails.queryAttributes, plan.getQuery().getQueryAttributes());

    thriftPlanDetails.queryCounters = new HashMap<String, Long>();
    setMapValForLong(thriftPlanDetails.queryCounters, plan.getQuery().getQueryCounters());

    thriftPlanDetails.stageGraph = new GraphInfo();
    setStageGraph(thriftPlanDetails.stageGraph, plan.getQuery().getStageGraph());

    thriftPlanDetails.stageList = new ArrayList<StageInfo>();
    setStageList(thriftPlanDetails.stageList, plan.getQuery().getStageList());
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
    for (Stage stage : stageList) {
      StageInfo stageEnt = new StageInfo();
      stageEnt.stageId = stage.getStageId();
      stageEnt.stageType = stage.getStageType().toString();

      stageEnt.stageAttributes = new HashMap<String, String>();
      setMapValForString(stageEnt.stageAttributes, stage.getStageAttributes());

      stageEnt.stageCounters = new HashMap<String, Long>();
      setMapValForLong(stageEnt.stageCounters, stage.getStageCounters());

      stageEnt.taskList = new ArrayList<TaskInfo>();
      setTaskList(stageEnt.taskList, stage.getTaskList());

      stageEnt.done = stage.isDone();
      stageEnt.started = stage.isStarted();
      thriftStageList.add(stageEnt);
    }
  }

  private static void setTaskList(List<TaskInfo> thriftTaskList, List<Task> taskList) {
    if (taskList == null) {
      return;
    }
    for (Task task : taskList) {
      TaskInfo thriftTaskListEnt = new TaskInfo();
      thriftTaskListEnt.taskId = task.getTaskId();
      thriftTaskListEnt.taskType = task.getTaskType().toString();
      thriftTaskListEnt.done = task.isDone();
      thriftTaskListEnt.started = task.isStarted();

      thriftTaskListEnt.taskAttributes = new HashMap<String, String>();
      setMapValForString(thriftTaskListEnt.taskAttributes, task.getTaskAttributes());

      thriftTaskListEnt.taskCounters = new HashMap<String, Long>();
      setMapValForLong(thriftTaskListEnt.taskCounters, task.getTaskCounters());

      thriftTaskListEnt.operatorGraph = new GraphInfo();
      setOperatorGraph(thriftTaskListEnt.operatorGraph, task.getOperatorGraph());

      thriftTaskListEnt.operatorList = new ArrayList<OperatorInfo>();
      setOperatorList(thriftTaskListEnt.operatorList, task.getOperatorList());

      thriftTaskList.add(thriftTaskListEnt);
    }
  }

  private static void setTaskProgress(List<TaskDetailInfo> thriftTaskProgress, ArrayList<QueryStats.taskDetail> taskProgress) {
    if (taskProgress == null) {
      return;
    }
    for (QueryStats.taskDetail tDetail : taskProgress) {
      TaskDetailInfo thriftProgressInfo = new TaskDetailInfo();
      thriftProgressInfo.timeStamp = tDetail.getTimeStamp();
      thriftProgressInfo.progress = tDetail.getProgress();
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
    for (Adjacency adjacency : adjacencyList) {
      AdjacencyInfo adjacencyInfo = new AdjacencyInfo();
      adjacencyInfo.node = adjacency.getNode();
      adjacencyInfo.children = adjacency.getChildren();
      adjacencyInfo.adjacencyType = adjacency.getAdjacencyType().toString();
      thriftAdjacencyList.add(adjacencyInfo);
    }
  }

  private static void setOperatorList(List<OperatorInfo> thriftOperatorList, List<Operator> operatorList) {
    if (operatorList == null) {
      return;
    }
    for (Operator operator : operatorList) {
      OperatorInfo operatorInfo = new OperatorInfo();
      operatorInfo.operatorId = operator.getOperatorId();
      operatorInfo.operatorType = operator.getOperatorType().toString();
      operatorInfo.done = operator.isDone();
      operatorInfo.started = operator.isStarted();
      operatorInfo.operatorAttributes = new HashMap<String, String>();
      setMapValForString(operatorInfo.operatorAttributes, operator.getOperatorAttributes());
      operatorInfo.operatorCounters = new HashMap<String, Long>();
      setMapValForLong(operatorInfo.operatorCounters, operator.getOperatorCounters());
      thriftOperatorList.add(operatorInfo);
    }
  }
}
