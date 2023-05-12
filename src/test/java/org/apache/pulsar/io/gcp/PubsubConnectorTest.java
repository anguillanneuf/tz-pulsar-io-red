// Copyright 2023 Tianzi Cai
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.pulsar.io.gcp;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slf4j
public class PubsubConnectorTest {
  private static final String PULSAR_TOPIC = "persistent://public/default/test";
  private static final String PULSAR_PRODUCER = "pulsar_producer";
  private static final String PULSAR_CONSUMER = "pulsar_consumer";
  private static final String SUFFIX = UUID.randomUUID().toString().substring(0, 6);
  private static final String PROJECT_ID = System.getenv("PROJECT_ID");
  private static final String PUBSUB_RESOURCE_ID = System.getenv("TOPIC_ID");
  private static PulsarClient pulsarClient = null;
  private static Producer<String> pulsarProducer = null;
  private static Consumer<String> pulsarConsumer = null;
  private static Subscriber subscriber = null;
  private static Publisher publisher = null;

  static MessageReceiver receiver =
      (PubsubMessage message, AckReplyConsumer consumer) -> {
        assertTrue(message.getData().toStringUtf8().contains("msg"));
        consumer.ack();
      };

  @BeforeClass
  public static void setUp() throws PulsarClientException {

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bout);
    System.setOut(out);

    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      TopicName topicName = TopicName.of(PROJECT_ID, PUBSUB_RESOURCE_ID);
      SubscriptionName subscriptionName =
          SubscriptionName.of(PROJECT_ID, PUBSUB_RESOURCE_ID + SUFFIX);
      Subscription subscription =
          subscriptionAdminClient.createSubscription(
              subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
      publisher = Publisher.newBuilder(topicName).build();
      subscriber = Subscriber.newBuilder(subscription.getName(), receiver).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

    pulsarProducer =
        pulsarClient
            .newProducer(Schema.STRING)
            .topic(PULSAR_TOPIC)
            .producerName(PULSAR_PRODUCER)
            .create();
  }

  @AfterClass
  public static void tearDown() throws PulsarClientException {
    System.setOut(null);

    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      SubscriptionName subscriptionName =
          SubscriptionName.of(PROJECT_ID, PUBSUB_RESOURCE_ID + SUFFIX);
      subscriptionAdminClient.deleteSubscription(subscriptionName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (pulsarClient != null) {
      pulsarClient.close();
    }
    if (pulsarProducer != null) {
      pulsarProducer.close();
    }
    if (pulsarConsumer != null) {
      pulsarConsumer.close();
    }
  }

  @Test
  public void testPubsubSink() throws IOException {
    pulsarProducer.send("msg");

    try {
      subscriber.startAsync().awaitRunning();
      subscriber.awaitTerminated(30, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      subscriber.stopAsync();
    }
  }

  @Test
  public void testPubsubSource() {
    try {
      String msgId =
          publisher
              .publish(PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("abc")).build())
              .get();
      System.out.println(msgId);

      pulsarConsumer =
          pulsarClient
              .newConsumer(Schema.STRING)
              .topic(PULSAR_TOPIC)
              .subscriptionName(PULSAR_TOPIC + "-" + SUFFIX)
              .messageListener(
                  (consumer, msg) -> {
                    try {
                      assertTrue(Arrays.toString(msg.getData()).contains("abc"));
                      consumer.acknowledge(msg);
                    } catch (Exception e) {
                      consumer.negativeAcknowledge(msg);
                    }
                  })
              .consumerName(PULSAR_CONSUMER)
              .subscribe();

    } catch (ExecutionException | InterruptedException | PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }
}
