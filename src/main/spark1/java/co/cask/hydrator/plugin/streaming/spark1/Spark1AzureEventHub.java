/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.streaming.spark1;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.format.RecordFormats;
import com.google.common.base.Strings;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;
import scala.reflect.ClassTag$;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Spark 1 Azure Event Hub Streaming source
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("AzureEventHub")
@Description("Azure Event Hub Streaming Source.")
public class Spark1AzureEventHub extends StreamingSource<StructuredRecord> {

  private final Spark1AzureConfig conf;

  public Spark1AzureEventHub(Spark1AzureConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    conf.validate();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(conf.getSchema());
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {

    String policyName = conf.policyName;
    String policyKey = conf.policyKey;
    String eventHubNamespace = conf.namespace;
    String eventHubName = conf.name;
    String dir = conf.checkpointDirectory;
    String partitions = conf.partitionCount;
    String consumerGroup = Strings.isNullOrEmpty(conf.consumerGroup) ? "$Default" : conf.consumerGroup;
    String interval = Strings.isNullOrEmpty(conf.checkpointInterval) ? "10" : conf.checkpointInterval;
    Map<String, String> map = conf.getPerPartitionOffsets(conf.offset);

    DStream<byte[]> dStream = Spark1EventHubStreamFactory.createEventHubStream(streamingContext
                                                                                 .getSparkStreamingContext().ssc(),
                                                                         eventHubNamespace, eventHubName,
                                                                         policyName, policyKey, partitions, "0", dir,
                                                                         interval, consumerGroup, map.get("0"));

    for (int i = 1; i < Integer.parseInt(partitions); i++) {
      dStream = dStream.union(Spark1EventHubStreamFactory.createEventHubStream(streamingContext
                                                                                 .getSparkStreamingContext().ssc(),
                                                                         eventHubNamespace, eventHubName,
                                                                         policyName, policyKey, partitions,
                                                                         Integer.toString(i), dir, interval,
                                                                               consumerGroup,
                                                                               map.get(Integer.toString(i))));
    }

    return JavaDStream.fromDStream(dStream, ClassTag$.MODULE$.<byte[]>apply(byte[].class))
      .map(new Function<byte[], StructuredRecord>() {
        private transient RecordFormat<StreamEvent, StructuredRecord> recordFormat;

        @Override
        public StructuredRecord call(byte[] event) throws Exception {
          Schema schema = conf.getSchema();
          StructuredRecord.Builder builder = StructuredRecord.builder(schema);

          if (!Strings.isNullOrEmpty(conf.format)) {
            // first time this was called, initialize record format
            if (recordFormat == null) {
              FormatSpecification spec =
                new FormatSpecification(conf.format, schema, new HashMap<String, String>());
              recordFormat = RecordFormats.createInitializedFormat(spec);
            }

            StructuredRecord messageRecord = recordFormat.read(new StreamEvent(ByteBuffer.wrap(event)));
            for (Schema.Field field : messageRecord.getSchema().getFields()) {
              String fieldName = field.getName();
              builder.set(fieldName, messageRecord.get(fieldName));
            }
          }
          else {
            builder.set(schema.getFields().get(0).getName(), event);
          }

          return builder.build();
        }
      });
  }

  @Override
  public int getRequiredExecutors() {
    return Integer.parseInt(conf.partitionCount);
  }
}
