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

package com.twitter.elephantbird.mapred.input;

import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

@SuppressWarnings("deprecation")
public class HiveMultiInputFormat
    extends DeprecatedFileInputFormatWrapper<LongWritable, BinaryWritable>
    implements CombineHiveInputFormat.AvoidSplitCombination
{
  public HiveMultiInputFormat()
  {
    super(new MultiInputFormat());
  }

  private void initialize(FileSplit split, JobConf job) throws IOException
  {
    String thriftClassName = job.get(serdeConstants.SERIALIZATION_CLASS);
    if (thriftClassName != null && thriftClassName.equals("com.facebook.presto.twitter.hive.thrift.ThriftGenericRow"))
      thriftClassName = "org.apache.hadoop.hive.ql.io.thrift.twitter.ThriftGenericRow";

    try {
      Class thriftClass = job.getClassByName(thriftClassName);
      setInputFormatInstance(new MultiInputFormat(new TypeRef(thriftClass) {}));
    }
    catch (ClassNotFoundException e) {
      throw new IOException("Failed getting class for " + thriftClassName);
    }
  }

  @Override
  public boolean isSplitable(FileSystem fs, Path filename)
  {
    if (filename.toString().endsWith(".lzo")) {
      Path indexFile = filename.suffix(".index");
      try {
        return fs.exists(indexFile);
      }
      catch (IOException e) {
        return false;
      }
    }
    return super.isSplitable(fs, filename);
  }

  @Override
  public RecordReader<LongWritable, BinaryWritable> getRecordReader(
      InputSplit split,
      JobConf job,
      Reporter reporter)
      throws IOException
  {
    initialize((FileSplit) split, job);
    return super.getRecordReader(split, job, reporter);
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf)
      throws IOException {
    // Skip combine for all paths
    return true;
  }
}
