/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.plugin.streaming.spark2;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.flow.flowlet.StreamEvent;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.format.RecordFormats;
import com.google.common.base.Strings;
import com.microsoft.azure.eventhubs.EventData;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.InputDStream;
import scala.reflect.ClassTag$;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Spark 2 Azure Event Hub Streaming source
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("AzureEventHub")
@Description("Azure Event Hub Streaming Source.")
public class Spark2AzureEventHub extends StreamingSource<StructuredRecord> {

  private final Spark2AzureConfig conf;

  public Spark2AzureEventHub(Spark2AzureConfig conf) {
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
    String maxRate = Strings.isNullOrEmpty(conf.maxRate) ? "100" : conf.maxRate;

    InputDStream<EventData> eventHubStream =
      Spark2EventHubStreamFactory.createEventHubStream(streamingContext.getSparkStreamingContext().ssc(),
                                                       eventHubNamespace, eventHubName, policyName, policyKey,
                                                       partitions, dir, maxRate, consumerGroup);

    return JavaDStream.fromDStream(eventHubStream, ClassTag$.MODULE$.<EventData>apply(EventData.class))
      .map(new Function<EventData, StructuredRecord>() {
        private transient RecordFormat<StreamEvent, StructuredRecord> recordFormat;

        @Override
        public StructuredRecord call(EventData event) throws Exception {
          Schema schema = conf.getSchema();
          StructuredRecord.Builder builder = StructuredRecord.builder(schema);

          if (!Strings.isNullOrEmpty(conf.format)) {
            // first time this was called, initialize record format
            if (recordFormat == null) {
              FormatSpecification spec =
                new FormatSpecification(conf.format, schema, new HashMap<String, String>());
              recordFormat = RecordFormats.createInitializedFormat(spec);
            }


            StructuredRecord messageRecord = recordFormat.read(new StreamEvent(ByteBuffer.wrap(event.getBytes())));
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
