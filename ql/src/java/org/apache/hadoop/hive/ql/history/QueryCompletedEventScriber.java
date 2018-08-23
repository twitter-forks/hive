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

import org.apache.hadoop.hive.ql.thrift.hive.AdjacencyInfo;
import org.apache.hadoop.hive.ql.thrift.hive.GraphInfo;
import org.apache.hadoop.hive.ql.thrift.hive.HiveQueryCompletionEvent;
import org.apache.hadoop.hive.ql.thrift.hive.OperatorInfo;
import org.apache.hadoop.hive.ql.thrift.hive.PlanDetails;
import org.apache.hadoop.hive.ql.thrift.hive.PlanInfo;
import org.apache.hadoop.hive.ql.thrift.hive.QueryStageInfo;
import org.apache.hadoop.hive.ql.thrift.hive.StageInfo;
import org.apache.hadoop.hive.ql.thrift.hive.TaskDetailInfo;
import org.apache.hadoop.hive.ql.thrift.hive.TaskInfo;

/**
 * Class that scribes query completion events
 */
public class QueryCompletedEventScriber {

  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.exec.HiveScribeImpl");

  private TwitterScriber scriber = new TwitterScriber("hive_query_completion");

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
    thriftEvent.setQueryId(event.getQueryId());
    thriftEvent.setQueryString(event.getQueryString());
    thriftEvent.setStartTime(event.getStartTime());
    thriftEvent.setEndTime(event.getEndTime());
    thriftEvent.setUser(event.getUsername());
    thriftEvent.setIp(event.getIPAddress());
    thriftEvent.setSessionId(event.getSessionID());
    thriftEvent.setDatabase(event.getDatabase());
    thriftEvent.setPlanProgress(new ArrayList<>());
    thriftEvent.setTaskProgress(new ArrayList<>());
    thriftEvent.setMapReduceStats(new HashMap<>());

    setPlanProgress(thriftEvent.getPlanProgress(), event.getPlanProgress());
    setTaskProgress(thriftEvent.getTaskProgress(), event.getTaskProgress());
    setMapReduceStats(thriftEvent.getMapReduceStats(), event.getMapReduceStats());

    return thriftEvent;
  }

  /**
   * Update plansInfo for thrift object according to pre-defined schema
   */
  private static void setPlanProgress(List<PlanInfo> thriftPlansInfo, ArrayList<QueryStats.plan> planProgress) {
    if (planProgress == null) {
      return;
    }
    for (QueryStats.plan planEnt : planProgress) {
      PlanInfo thriftPlanInfo = new PlanInfo();
      thriftPlanInfo.setTimeStamp(planEnt.getTimeStamp());
      thriftPlanInfo.setPlanDetails(new PlanDetails());
      setPlanDetails(thriftPlanInfo.getPlanDetails(), planEnt.getQueryPlan());
      thriftPlansInfo.add(thriftPlanInfo);
    }
  }

  private static void setPlanDetails(PlanDetails thriftPlanDetails, QueryPlan plan) {
    thriftPlanDetails.setQueryId(plan.getQueryId());
    thriftPlanDetails.setQueryType(plan.getQuery().getQueryType());
    thriftPlanDetails.setDone(plan.getDone().toString());
    thriftPlanDetails.setStarted(plan.getStarted().toString());

    thriftPlanDetails.setQueryAttributes(new HashMap<>());
    setMapValForString(thriftPlanDetails.getQueryAttributes(), plan.getQuery().getQueryAttributes());

    thriftPlanDetails.setQueryCounters(new HashMap<>());
    setMapValForLong(thriftPlanDetails.getQueryCounters(), plan.getQuery().getQueryCounters());

    thriftPlanDetails.setStageGraph(new GraphInfo());
    setStageGraph(thriftPlanDetails.getStageGraph(), plan.getQuery().getStageGraph());

    thriftPlanDetails.setStageList(new ArrayList<>());
    setStageList(thriftPlanDetails.getStageList(), plan.getQuery().getStageList());
  }

  private static void setStageGraph(GraphInfo thriftStageGraph, Graph stageGraph) {
    if (stageGraph == null) {
      return;
    }
    thriftStageGraph.setNodeType(stageGraph.getNodeType().toString());

    if (stageGraph.getRoots() != null) {
      thriftStageGraph.setRoots(new ArrayList<>());
      thriftStageGraph.getRoots().addAll(stageGraph.getRoots());
    }

    thriftStageGraph.setAdjacencyList(new ArrayList<>());
    for (int i = 0; i < stageGraph.getAdjacencyListSize(); i++) {
      AdjacencyInfo adjacencyListEnt = new AdjacencyInfo();
      adjacencyListEnt.setNode(stageGraph.getAdjacencyList().get(i).getNode());
      adjacencyListEnt.setChildren(stageGraph.getAdjacencyList().get(i).getChildren());
      adjacencyListEnt.setAdjacencyType(stageGraph.getAdjacencyList().get(i).getAdjacencyType().toString());
      thriftStageGraph.getAdjacencyList().add(adjacencyListEnt);
    }
  }

  private static void setMapReduceStats(Map<String, QueryStageInfo> thriftMapReduceInfo, Map<String, MapRedStats> mapReduceInfo) {
    if (mapReduceInfo == null) {
      return;
    }
    for (Map.Entry<String, MapRedStats> ent : mapReduceInfo.entrySet()) {
      QueryStageInfo thriftCounterInfo = new QueryStageInfo();
      String key = ent.getKey();
      thriftCounterInfo.setStageId(key);
      thriftCounterInfo.setJobId(ent.getValue().getJobId());
      thriftCounterInfo.setCpuMsec(ent.getValue().getCpuMSec());
      thriftCounterInfo.setCounters(ent.getValue().getCounters().toString());
      thriftCounterInfo.setNumberMappers(ent.getValue().getNumMap());
      thriftCounterInfo.setNumberReducers(ent.getValue().getNumReduce());
      thriftCounterInfo.setTaskNumbers(ent.getValue().getTaskNumbers());
      thriftMapReduceInfo.put(key, thriftCounterInfo);
    }
  }

  private static void setStageList(List<StageInfo> thriftStageList, List<Stage> stageList) {
    if (stageList == null) {
      return;
    }
    for (Stage stage : stageList) {
      StageInfo stageEnt = new StageInfo();
      stageEnt.setStageId(stage.getStageId());
      stageEnt.setStageType(stage.getStageType().toString());

      stageEnt.setStageAttributes(new HashMap<>());
      setMapValForString(stageEnt.getStageAttributes(), stage.getStageAttributes());

      stageEnt.setStageCounters(new HashMap<>());
      setMapValForLong(stageEnt.getStageCounters(), stage.getStageCounters());

      stageEnt.setTaskList(new ArrayList<>());
      setTaskList(stageEnt.getTaskList(), stage.getTaskList());

      stageEnt.setDone(stage.isDone());
      stageEnt.setStarted(stage.isStarted());
      thriftStageList.add(stageEnt);
    }
  }

  private static void setTaskList(List<TaskInfo> thriftTaskList, List<Task> taskList) {
    if (taskList == null) {
      return;
    }
    for (Task task : taskList) {
      TaskInfo thriftTaskListEnt = new TaskInfo();
      thriftTaskListEnt.setTaskId(task.getTaskId());
      thriftTaskListEnt.setTaskType(task.getTaskType().toString());
      thriftTaskListEnt.setDone(task.isDone());
      thriftTaskListEnt.setStarted(task.isStarted());

      thriftTaskListEnt.setTaskAttributes(new HashMap<>());
      setMapValForString(thriftTaskListEnt.getTaskAttributes(), task.getTaskAttributes());

      thriftTaskListEnt.setTaskCounters(new HashMap<>());
      setMapValForLong(thriftTaskListEnt.getTaskCounters(), task.getTaskCounters());

      thriftTaskListEnt.setOperatorGraph(new GraphInfo());
      setOperatorGraph(thriftTaskListEnt.getOperatorGraph(), task.getOperatorGraph());

      thriftTaskListEnt.setOperatorList(new ArrayList<>());
      setOperatorList(thriftTaskListEnt.getOperatorList(), task.getOperatorList());

      thriftTaskList.add(thriftTaskListEnt);
    }
  }

  private static void setTaskProgress(List<TaskDetailInfo> thriftTaskProgress, ArrayList<QueryStats.taskDetail> taskProgress) {
    if (taskProgress == null) {
      return;
    }
    for (QueryStats.taskDetail tDetail : taskProgress) {
      TaskDetailInfo thriftProgressInfo = new TaskDetailInfo();
      thriftProgressInfo.setTimeStamp(tDetail.getTimeStamp());
      thriftProgressInfo.setProgress(tDetail.getProgress());
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
    thriftOperatorGraph.setNodeType(operatorGraph.getNodeType().toString());
    if (operatorGraph.getRoots() != null) {
      thriftOperatorGraph.setRoots(new ArrayList<>());
      thriftOperatorGraph.getRoots().addAll(operatorGraph.getRoots());
    }

    thriftOperatorGraph.setAdjacencyList(new ArrayList<>());
    setAdjacencyList(thriftOperatorGraph.getAdjacencyList(), operatorGraph.getAdjacencyList());
  }

  private static void setAdjacencyList(List<AdjacencyInfo> thriftAdjacencyList, List<Adjacency> adjacencyList) {
    if (adjacencyList == null) {
      return;
    }
    for (Adjacency adjacency : adjacencyList) {
      AdjacencyInfo adjacencyInfo = new AdjacencyInfo();
      adjacencyInfo.setNode(adjacency.getNode());
      adjacencyInfo.setChildren(adjacency.getChildren());
      adjacencyInfo.setAdjacencyType(adjacency.getAdjacencyType().toString());
      thriftAdjacencyList.add(adjacencyInfo);
    }
  }

  private static void setOperatorList(List<OperatorInfo> thriftOperatorList, List<Operator> operatorList) {
    if (operatorList == null) {
      return;
    }
    for (Operator operator : operatorList) {
      OperatorInfo operatorInfo = new OperatorInfo();
      operatorInfo.setOperatorId(operator.getOperatorId());
      operatorInfo.setOperatorType(operator.getOperatorType().toString());
      operatorInfo.setDone(operator.isDone());
      operatorInfo.setStarted(operator.isStarted());
      operatorInfo.setOperatorAttributes(new HashMap<>());
      setMapValForString(operatorInfo.getOperatorAttributes(), operator.getOperatorAttributes());
      operatorInfo.setOperatorCounters(new HashMap<>());
      setMapValForLong(operatorInfo.getOperatorCounters(), operator.getOperatorCounters());
      thriftOperatorList.add(operatorInfo);
    }
  }
}
