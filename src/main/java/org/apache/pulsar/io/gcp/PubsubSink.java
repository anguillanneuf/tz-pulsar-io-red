package org.apache.pulsar.io.gcp;

import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

@Slf4j
public class PubsubSink implements Sink<GenericObject> {

  private Publisher publisher = null;

  @Override
  public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    PubsubSinkConfig pubsubSinkConfig = PubsubSinkConfig.load(config);
    TopicName topicName =
        TopicName.of(pubsubSinkConfig.getProjectId(), pubsubSinkConfig.getTopicId());
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setElementCountThreshold(pubsubSinkConfig.getBatchSize())
            .build();
    this.publisher = Publisher.newBuilder(topicName).setBatchingSettings(batchingSettings).build();
  }

  @Override
  public void write(Record<GenericObject> record) {
    if (record.getMessage().isPresent()) {
      ByteString data = ByteString.copyFrom(record.getMessage().get().getData());
      PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
      this.publisher.publish(pubsubMessage);
    }
  }

  @Override
  public void close() {
    if (this.publisher != null) {
      this.publisher.shutdown();
      log.info("Pub/Sub sink has been shut down.");
    }
  }
}
