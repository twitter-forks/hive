namespace java org.apache.hadoop.hive.ql.thrift.hive
#@namespace scala org.apache.hadoop.hive.ql.thrifthive

/**
 * Thrift version of a Presto QueryCompletionEvent. See:
 * https://github.com/prestodb/presto/blob/master/presto-main/src/main/java/com/facebook/presto/event/query/QueryCompletionEvent.java
 */

struct OperatorInfo {
  1: optional string operatorId
  2: optional string operatorType
  3: optional map<string,string> operatorAttributes
  4: optional map<string,i64> operatorCounters
  5: optional bool done
  6: optional bool started
}(persisted='true')

struct AdjacencyInfo {
  1: optional string node
  2: optional list<string> children
  3: optional string adjacencyType
}(persisted='true')

struct GraphInfo {
  1: optional string nodeType
  2: optional list<string> roots
  3: optional list<AdjacencyInfo> adjacencyList
}(persisted='true')

struct TaskInfo {
  1: optional string taskId
  2: optional string taskType
  3: optional map<string,string> taskAttributes
  4: optional map<string,i64> taskCounters
  5: optional GraphInfo operatorGraph
  6: optional list<OperatorInfo> operatorList
  7: optional bool done
  8: optional bool started
}(persisted='true')

struct StageInfo {
  1: optional string stageId
  2: optional string stageType
  3: optional map<string,string> stageAttributes
  4: optional map<string,i64> stageCounters
  5: optional list<TaskInfo> taskList
  6: optional bool done
  7: optional bool started
}(persisted='true')

struct PlanDetails {
  1: optional string queryId
  2: optional string queryType
  3: optional map<string,string> queryAttributes
  4: optional map<string,i64> queryCounters
  5: optional GraphInfo stageGraph
  6: optional list<StageInfo> stageList
  7: optional string done
  8: optional string started
}(persisted='true')

struct PlanInfo {
  1: optional i64 timeStamp
  2: optional PlanDetails planDetails
}(persisted='true')

struct QueryStageInfo {
    1: optional string stageId
    2: optional string jobId
    3: optional i64 cpuMsec
    4: optional string counters
    5: optional i32 numberMappers
    6: optional i32 numberReducers
    7: optional string taskNumbers
}(persisted='true')

struct TaskDetailInfo {
    1: optional i64 timeStamp
    2: optional string progress
}(persisted='true')

struct HiveQueryCompletionEvent {
    1: required string queryId
    2: required string user
    3: required string ip
    4: required string sessionId
    5: required string database
    6: required i64 startTime
    7: required i64 endTime
    8: required string queryString
    9: optional map<string, QueryStageInfo> mapReduceStats
   10: optional list<PlanInfo> planProgress
   11: optional list<TaskDetailInfo> taskProgress
}(persisted='true')
