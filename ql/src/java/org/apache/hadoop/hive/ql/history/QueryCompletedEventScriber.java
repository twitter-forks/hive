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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.QueryStats;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hive.thriftjava.QueryCompletionEvent;
import com.twitter.hive.thriftjava.QueryStageInfo;

/**
 * Class that scribes query completion events
 */
public class QueryCompletedEventScriber {

  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.exec.HiveScribeImpl");

  private static final String DASH = "-";

  private TwitterScriber scriber = new TwitterScriber("test_hive_query_completion");

  public void handle(QueryStats event) {
    try {
      scriber.scribe(toThriftQueryCompletionEvent(event));
    } catch (TException e) {
      LOG.warn(String.format("%s, %s", e,
          String.format("Could not serialize thrift object of Query(id=%s, user=%s, session=%s, database=%s)",
              event.queryID,
              event.username,
              event.sessionID,
              event.database)));
    }
  }

  private static QueryCompletionEvent toThriftQueryCompletionEvent(QueryStats event) {
    QueryCompletionEvent thriftEvent =
        new com.twitter.hive.thriftjava.QueryCompletionEvent();

    thriftEvent.query_id = event.queryID;
    thriftEvent.user = event.username;
    thriftEvent.ip = event.IPAddress;
    thriftEvent.session_id = event.sessionID;
    thriftEvent.database = event.database;
    thriftEvent.start_time = event.queryStart;
    thriftEvent.end_time = event.queryEnd;
    thriftEvent.query_string = event.queryString;
    thriftEvent.map_reduce_info = new HashMap<String, QueryStageInfo>();

    for (Map.Entry<String, MapRedStats> ent : event.mapReduceStats.entrySet()) {
      QueryStageInfo thriftCounterInfo = new com.twitter.hive.thriftjava.QueryStageInfo();
      String key = ent.getKey();
      thriftCounterInfo.stage_id = key;
      thriftCounterInfo.job_id = ent.getValue().getJobId().toString();
      thriftCounterInfo.cpu_msec = ent.getValue().getCpuMSec();
      thriftCounterInfo.counters = ent.getValue().getCounters().toString();
      thriftCounterInfo.number_mappers = ent.getValue().getNumMap();
      thriftCounterInfo.number_reducers = ent.getValue().getNumReduce();
      thriftCounterInfo.task_numbers = ent.getValue().getTaskNumbers().toString();
      thriftEvent.map_reduce_info.put(key, thriftCounterInfo);
    }
    return thriftEvent;
  }
}
