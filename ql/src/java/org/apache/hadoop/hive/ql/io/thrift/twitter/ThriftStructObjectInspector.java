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

package org.apache.hadoop.hive.ql.io.thrift.twitter;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.BaseStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

public class ThriftStructObjectInspector extends BaseStructObjectInspector {

  ThriftFieldIdResolver resolver;

  public ThriftStructObjectInspector(List<String> fieldNames, List<ObjectInspector> loi, ThriftFieldIdResolver resolver) {
    super(fieldNames, loi);
    this.resolver = resolver;
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef)
  {
    ThriftGenericRow row = (ThriftGenericRow) data;
    return row.getFieldValueForThriftId(resolver.getThriftId(fieldRef.getFieldID()));
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data)
  {
    ThriftGenericRow row = (ThriftGenericRow) data;
    List<Object> allValues = new ArrayList<>();
    for (StructField fieldRef : getAllStructFieldRefs())
      allValues.add(row.getFieldValueForThriftId(resolver.getThriftId(fieldRef.getFieldID())));

    return allValues;
  }

  public int getThriftId(StructField fieldRef)
  {
    return (int) resolver.getThriftId(fieldRef.getFieldID());
  }
}
