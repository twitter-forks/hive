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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class HiveThriftFieldIdResolverFactory
{
  private static final Logger log = LoggerFactory.getLogger(HiveThriftFieldIdResolverFactory.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  public static final String THRIFT_FIELD_ID_JSON = "thrift.field.id.json";
  // The default resolver which returns thrift id as hive id plus one
  public static final ThriftFieldIdResolver HIVE_THRIFT_FIELD_ID_DEFAULT_RESOLVER = new HiveThriftFieldIdResolver(null);

  public static ThriftFieldIdResolver createResolver(Properties schema)
  {
    String jsonData = schema.getProperty(THRIFT_FIELD_ID_JSON);
    if (jsonData == null) {
      return HIVE_THRIFT_FIELD_ID_DEFAULT_RESOLVER;
    }

    try {
      JsonNode root = objectMapper.readTree(jsonData);
      return new HiveThriftFieldIdResolver(root);
    }
    catch (IOException e) {
      log.debug(e.toString() + "Failed to create an optimized thrift id resolver, json string: %s, schema: %s. Will use a default resolver." + jsonData + schema);
    }

    return HIVE_THRIFT_FIELD_ID_DEFAULT_RESOLVER;
  }
}
