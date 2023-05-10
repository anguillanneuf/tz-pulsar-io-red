package org.apache.pulsar.io.gcp;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.io.PrintStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@Slf4j
public class PubsubConnectorTest {
  private static final String PULSAR_TOPIC = "persistent://public/default/test";
  private static final String PULSAR_PRODUCER = "pulsar_producer";
  private static final String SUFFIX = UUID.randomUUID().toString().substring(0, 6);
  private static final String PROJECT_ID = System.getenv("PROJECT_ID");
  private static final String PUBSUB_RESOURCE_ID = System.getenv("TOPIC_ID");
  private static PulsarClient pulsarClient = null;
  private static Producer<String> pulsarProducer = null;
  private static Subscription subscription = null;
  private static Subscriber subscriber = null;

  MessageReceiver receiver =
      (PubsubMessage message, AckReplyConsumer consumer) -> {
        assertTrue(message.getData().toStringUtf8().contains("msg"));
        consumer.ack();
      };

  @BeforeClass
  public static void setUp() throws PulsarClientException {
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      TopicName topicName = TopicName.of(PROJECT_ID, PUBSUB_RESOURCE_ID);
      SubscriptionName subscriptionName = SubscriptionName.of(PROJECT_ID, PUBSUB_RESOURCE_ID+SUFFIX);
      subscription =
          subscriptionAdminClient.createSubscription(
              subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
    } catch (IOException e) {
      log.info("Failed to create subscription.");
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
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      SubscriptionName subscriptionName = SubscriptionName.of(PROJECT_ID, PUBSUB_RESOURCE_ID+SUFFIX);
      subscriptionAdminClient.deleteSubscription(subscriptionName);
    } catch (IOException e) {
      log.info("Failed to delete subscription.");
      throw new RuntimeException(e);
    }

    if (pulsarClient != null) {
      pulsarClient.close();
    }
    if (pulsarProducer != null) {
      pulsarProducer.close();
    }
  }

  @Test
  public void testPubsubSink() throws IOException {
    pulsarProducer.send("msg");

    try {
      subscriber = Subscriber.newBuilder(subscription.getName(), receiver).build();
      subscriber.startAsync().awaitRunning();
      subscriber.awaitTerminated(30, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      subscriber.stopAsync();
    }
  }
}
