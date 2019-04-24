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
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.format.RecordFormats;
import io.cdap.plugin.common.KeyValueListParser;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Config for Azure Event hub source
 */
public class Spark2AzureConfig extends PluginConfig implements Serializable {


  private static final long serialVersionUID = -6508730404068826870L;
  @Description("Azure Namespace for the event hub.")
  @Macro
  public String namespace;

  @Description("Name of the Azure Event Hub to read messages from. The source will read events from all the partitions")
  @Macro
  public String name;

  @Description("Name of provided Event Hub Shared Access Policy with Listen permissions.")
  @Macro
  public String policyName;

  @Description("Primary key of Shared Access Policy")
  @Macro
  public String policyKey;

  @Description("Number of partitions of provided Event Hub. Please make sure this number is right otherwise some " +
    "messages may not be consumed")
  @Macro
  public String partitionCount;

  @Description("HDFS directory location where offsets for each partitions will be stored.")
  @Macro
  public String checkpointDirectory;

  @Description("This parameter regulates the maximum number of messages being processed in a single batch for every " +
    "EventHub partition. and it effectively prevent the job being hold due to the large number of messages being " +
    "fetched at once. Default is 100")
  @Macro
  @Nullable
  public String maxRate;

  @Description("Event hub consumer group name, defaults to $default")
  @Macro
  @Nullable
  public String consumerGroup;

  @Description("Optional format of the event. Any format supported by CDAP is supported. " +
    "For example, a value of 'csv' will attempt to parse Azure events as comma-separated values. " +
    "If no format is given, all the events will be treated as byes.")
  @Nullable
  public String format;

  @Description("Output schema of the source.")
  public String schema;


  public Spark2AzureConfig(String namespace, String name, String policyName, String policyKey,
                           String partitionCount, String checkpointDirectory, String maxRate,
                           String consumerGroup, String format, String schema) {
    this.namespace = namespace;
    this.name = name;
    this.policyName = policyName;
    this.policyKey = policyKey;
    this.partitionCount = partitionCount;
    this.checkpointDirectory = checkpointDirectory;
    this.maxRate = maxRate;
    this.consumerGroup = consumerGroup;
    this.format = format;
    this.schema = schema;
  }

  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
    }
  }

  public Map<String, String> getPerPartitionOffsets(String offset) {
    Map<String, String> map = new HashMap<>();
    int partitionCount = Integer.parseInt(this.partitionCount);

    // default value for partition offset is -1
    for (int i = 0; i < partitionCount; i++) {
      map.put(Integer.toString(i), "-1");
    }

    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    if (!Strings.isNullOrEmpty(offset)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(offset)) {
        map.put(keyVal.getKey(), keyVal.getValue());
      }
    }
    return map;
  }

  public void validate() {
    Schema schema = getSchema();
    // if format is empty, there must be just a single message field of type bytes or nullable types.
    if (Strings.isNullOrEmpty(format)) {
      List<Schema.Field> messageFields = schema.getFields();
      if (messageFields.size() > 1) {
        List<String> fieldNames = new ArrayList<>();
        for (Schema.Field messageField : messageFields) {
          fieldNames.add(messageField.getName());
        }
        throw new IllegalArgumentException(String.format(
          "Without a format, the schema must contain just a single event field of type bytes or nullable bytes. " +
            "Found %s message fields (%s).", messageFields.size(), Joiner.on(',').join(fieldNames)));
      }

      Schema.Field messageField = messageFields.get(0);
      Schema messageFieldSchema = messageField.getSchema();
      Schema.Type messageFieldType = messageFieldSchema.isNullable() ?
        messageFieldSchema.getNonNullable().getType() : messageFieldSchema.getType();
      if (messageFieldType != Schema.Type.BYTES) {
        throw new IllegalArgumentException(String.format(
          "Without a format, the message field must be of type bytes or nullable bytes, but field %s is of type %s.",
          messageField.getName(), messageField.getSchema()));
      }
    } else {
      // otherwise, if there is a format, make sure we can instantiate it.
      FormatSpecification formatSpec = new FormatSpecification(format, schema, new HashMap<String, String>());

      try {
        RecordFormats.createInitializedFormat(formatSpec);
      } catch (Exception e) {
        throw new IllegalArgumentException(String.format(
          "Unable to instantiate a message parser from format '%s' and message schema '%s': %s",
          format, schema, e.getMessage()), e);
      }
    }

    try {
      int partitionCount = Integer.parseInt(this.partitionCount);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Invalid partition '%s'. Partitions must be integers.", partitionCount));
    }
  }
}
