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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;

import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldIdEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;

import junit.framework.TestCase;

public class TestThriftGeneralDeserializer extends TestCase {
  public static final Logger log = LoggerFactory.getLogger(TestThriftGeneralDeserializer.class);
  public void testThriftDeserializer() throws Throwable {
    try {
      log.info("test: testThriftDeserializer");

      // Create the thrift writable object
      Tweet tweet = new Tweet(1, "newUser", "hello world")
          .setLoc(new Location(1234, 5678))
          .setAge((short) 26)
          .setB((byte) 10)
          .setIsDeleted(false)
          .setTweetType(TweetType.REPLY)
          .setFullId(1234567)
          .setPic("abc".getBytes())
          .setAttr(ImmutableMap.of("a", "a"));

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ThriftWritable writable = ThriftWritable.newInstance(Tweet.class);
      writable.set(tweet);
      writable.write(new DataOutputStream(out));

      DataInput in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
      ThriftWritable newWritable = ThriftWritable.newInstance(ThriftGenericRow.class);
      newWritable.readFields(in);

      // Create object inspector hierarchy
      final ThriftGeneralDeserializer serDe = new ThriftGeneralDeserializer();
      final Configuration conf = new Configuration();
      final Properties tbl = createProperties();
      SerDeUtils.initializeSerDe(serDe, conf, tbl, null);

      // Deserialize
      final Object row = serDe.deserialize(newWritable);
      assertEquals("deserialization gives the wrong object class", row.getClass(), ThriftGenericRow.class);
      assertEquals("size incorrect after deserialization", serDe.getSerDeStats().getRawDataSize(), ((ThriftGenericRow) newWritable.get()).length());

      assertResult(row, tweet, (ThriftStructObjectInspector)serDe.getObjectInspector(), true);

      Tweet tweet1 = new Tweet(1, "newUser", "hello world")
          .setLoc(new Location(1234, 5678))
          .setAge((short) 26)
          .setB((byte) 10)
          .setIsDeleted(false)
          .setTweetType(TweetType.REPLY)
          .setFullId(1234568)
          .setPic("abc".getBytes())
          .setAttr(ImmutableMap.of("a", "a"));

      try
      {
        assertResult(row, tweet1, (ThriftStructObjectInspector)serDe.getObjectInspector(), false);
        fail("This test should not pass");
      }
      catch (AssertionError e)
      {
        assertEquals("This test failed for some other reason", e.getMessage(), "Field Type bigint does not match expected:<1234568> but was:<1234567>");
      }

    } catch (final Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void assertResult(final Object result, final T expectedResult, final ObjectInspector oi, final boolean testLazyDeserialize) throws AssertionError
  {
    switch (oi.getCategory())
    {
      case PRIMITIVE:
        if (expectedResult instanceof TEnum) {
          assertEquals("Field Type " + oi.getTypeName() + " does not match", ((TEnum) expectedResult).getValue(), result);
        }
        else if (expectedResult instanceof byte[]) {
          assertTrue("Field Type " + oi.getTypeName() + " does not match", Arrays.equals(((String) result).getBytes(), (byte[]) expectedResult));
        }
        else
          assertEquals("Field Type " + oi.getTypeName() + " does not match", expectedResult, result);

        break;
      case STRUCT:{
        ThriftStructObjectInspector soi = (ThriftStructObjectInspector) oi;
        TBase<?, TFieldIdEnum> tbase = ((TBase<?, TFieldIdEnum>) expectedResult);
        for (StructField field : soi.getAllStructFieldRefs()) {
          if (((ThriftGenericRow)result).isSet(((ThriftGenericRow)result).fieldForId((short) soi.getThriftId(field))))
          {
            if (testLazyDeserialize)
              assertEquals("Field should not be deserialized on first access", false, ((ThriftGenericRow)result).isDeserialized((short) soi.getThriftId(field)));
            assertResult(soi.getStructFieldData(result, field), tbase.getFieldValue(tbase.fieldForId(soi.getThriftId(field))), field.getFieldObjectInspector(), testLazyDeserialize);
            if (testLazyDeserialize)
              assertEquals("Field should be deserialized after first access", true, ((ThriftGenericRow)result).isDeserialized((short) soi.getThriftId(field)));
          }
        }
        break;
      }
      case LIST:{
        ListObjectInspector loi = (ListObjectInspector) oi;
        int length = loi.getListLength(result);
        for (int j = 0; j < length; j++) {
          assertResult(loi.getListElement(result, j), ((List<Object>) expectedResult).get(j), loi.getListElementObjectInspector(), testLazyDeserialize);
        }
        break;
      }
      case MAP:{
        MapObjectInspector moi = (MapObjectInspector) oi;
        Map<?, ?> tMap = (Map<?, ?>) expectedResult;
        for (Map.Entry<?, ?> mEntry : moi.getMap(result).entrySet())
        {
          assertEquals("Map key entry does not match", true, tMap.containsKey(mEntry.getKey()));
          assertResult(mEntry.getValue(), tMap.get(mEntry.getKey()), moi.getMapValueObjectInspector(), testLazyDeserialize);
        }
        break;
      }
      default:
      {
        assertTrue("Unknown data type", true);
      }
    }
  }

  private Properties createProperties() {
    StructTypeInfo tweepStruct = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(
        toList("userid", "username", "text", "loc", "tweettype", "isdeleted", "b", "age", "fullid", "pic", "attr", "items", "language"),
        toList(getPrimitiveTypeInfo(INT_TYPE_NAME),
            getPrimitiveTypeInfo(STRING_TYPE_NAME),
            getPrimitiveTypeInfo(STRING_TYPE_NAME),
            TypeInfoFactory.getStructTypeInfo(
                toList("latitude", "longitude"),
                toList(getPrimitiveTypeInfo(DOUBLE_TYPE_NAME),
                    getPrimitiveTypeInfo(DOUBLE_TYPE_NAME))
                ),
            getPrimitiveTypeInfo(INT_TYPE_NAME),
            getPrimitiveTypeInfo(BOOLEAN_TYPE_NAME),
            getPrimitiveTypeInfo(TINYINT_TYPE_NAME),
            getPrimitiveTypeInfo(SMALLINT_TYPE_NAME),
            getPrimitiveTypeInfo(BIGINT_TYPE_NAME),
            getPrimitiveTypeInfo(BINARY_TYPE_NAME),
            TypeInfoFactory.getMapTypeInfo(
                getPrimitiveTypeInfo(STRING_TYPE_NAME),
                getPrimitiveTypeInfo(STRING_TYPE_NAME)
            ),
            TypeInfoFactory.getListTypeInfo(
                getPrimitiveTypeInfo(STRING_TYPE_NAME)),
            getPrimitiveTypeInfo(STRING_TYPE_NAME)
        )
    );

    final Properties tbl = new Properties();
    // Set the configuration parameters
    tbl.setProperty("columns", String.join(",",tweepStruct.getAllStructFieldNames()));
    tbl.setProperty("columns.types", String.join(":",tweepStruct.getAllStructFieldTypeInfos().stream().map(TypeInfo::toString).collect(Collectors.toList())));
    return tbl;
  }

  private <T> List<T> toList(T... array) {
    return Arrays.asList(array);
  }

  private TypeInfo getPrimitiveTypeInfo(String type) { return TypeInfoFactory.getPrimitiveTypeInfo(type); }
}
