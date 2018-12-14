/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftGeneralDeserializer implements Deserializer
{
  public static final Logger LOG = LoggerFactory.getLogger(ThriftGeneralDeserializer.class.getName());

  public ThriftGeneralDeserializer()
  { }

  List<String> columnNames;
  List<TypeInfo> columnTypes;

  ObjectInspector cachedObjectInspector;

  private SerDeStats stats;

  @Override
  public void initialize(Configuration job, Properties tbl)
      throws SerDeException {
    // Get column names and types
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils
          .getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (columnNames.size() != columnTypes.size()) {
      throw new SerDeException("ThriftGeneralDeserializer initialization failed. Number of column " +
          "name and column type differs. columnNames = " + columnNames + ", columnTypes = " +
          columnTypes);
    }

    ThriftFieldIdResolver resolver = HiveThriftFieldIdResolverFactory.createResolver(tbl);
    
    List<ObjectInspector> columnObjectInspectors = new ArrayList<>();
    for (int i = 0; i < columnNames.size(); i++)
      columnObjectInspectors.add(getObjectInspector(columnTypes.get(i), resolver.getNestedResolver(i)));

    cachedObjectInspector = new ThriftStructObjectInspector(columnNames, columnObjectInspectors, resolver);

    // output debug info
    LOG.debug("ThriftGeneralDeserializer initialized with: columnNames=" + columnNames
        + " columnTypes=" + columnTypes);

    stats = new SerDeStats();
  }

  private ObjectInspector getObjectInspector(TypeInfo info, ThriftFieldIdResolver resolver) {
    ObjectInspector result = null;
    switch (info.getCategory()) {
      case PRIMITIVE:
        result = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(TypeInfoFactory.getPrimitiveTypeInfo(info.getTypeName()));
        break;
      case LIST: {
        ObjectInspector elementObjectInspector = getObjectInspector(((ListTypeInfo) info)
            .getListElementTypeInfo(), resolver.getNestedResolver(0));
        result = ObjectInspectorFactory.getStandardListObjectInspector(elementObjectInspector);
        break;
      }
      case MAP: {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) info;
        ObjectInspector keyObjectInspector = getObjectInspector(mapTypeInfo
            .getMapKeyTypeInfo(), resolver.getNestedResolver(0));
        ObjectInspector valueObjectInspector = getObjectInspector(mapTypeInfo
            .getMapValueTypeInfo(), resolver.getNestedResolver(1));
        result = ObjectInspectorFactory
            .getStandardMapObjectInspector(keyObjectInspector,
                valueObjectInspector);
        break;
      }
      case STRUCT: {
        StructTypeInfo structTypeInfo = (StructTypeInfo) info;
        List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        List<TypeInfo> fieldTypeInfos = structTypeInfo
            .getAllStructFieldTypeInfos();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
            fieldTypeInfos.size());
        for (int i = 0; i < fieldTypeInfos.size(); i++) {
          fieldObjectInspectors
              .add(getObjectInspector(fieldTypeInfos.get(i), resolver.getNestedResolver(i)));
        }
        result = new ThriftStructObjectInspector(fieldNames, fieldObjectInspectors, resolver);
        break;
      }
      case UNION: {
        throw new UnsupportedOperationException("UNION is not supported right now.");
      }
      default:
        throw new IllegalArgumentException("Unknown type " +
            info.getCategory());
    }

    return result;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return stats;
  }

  public Object deserialize(Writable writable) throws SerDeException
  {
    ThriftGenericRow row = (ThriftGenericRow) ((ThriftWritable) writable).get();
    stats.setRawDataSize(row.length());
    try {
      row.parse();
    }
    catch (TException e) {
      throw new SerDeException("ThriftGenericRow failed to parse values", e);
    }
    return row;
  }
}
