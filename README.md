# Apache Pulsar Pub/Sub connector

A simple implementation of Pub/Sub sink and source connector to Apache Pulsar.

## How to run the sink connector

1. Prepare a Pub/Sub topic and subscription. The following examples make use of `projects/viva-magenta/topics/pantone` and `projects/viva-magenta/subscriptions/pantone`. 
2. Package the Pub/Sub sink connector with `mvn clean package -DskipTest=True`. Obtain `target/tz-pulsar-io-1.0-SNAPSHOT.nar`. Check out the contents with `jar xvf target/tz-pulsar-io-1.0-SNAPSHOT.nar`. Look out for:
   ```text
   org/apache/pulsar/io/gcp/PubsubConfig.class
   org/apache/pulsar/io/gcp/PubsubSink.class
   ```
3. Start a Pulsar cluster on Docker.
   ```shell
   # first time
   $ docker run -it -p 6650:6650 -p 8080:8080 --mount source=pulsardata,target=/pulsar/data --mount source=pulsarconf,target=/pulsar/conf apachepulsar/pulsar:2.11.1 bin/pulsar standalone

   # later times
   $ docker ps -a
   $ docker start ${CONTAINTER_ID}
   ```
4. Prepare the sink config keys and values. Check out [`tz-pubsub-sink.yaml`](src/test/resources/tz-pubsub-sink.yaml).
   ```shell
   $ cat examples/tz-pubsub-sink.yaml 
   tenant: "public"
   namespace: "default"
   name: "pubsub-sink"
   inputs: ["persistent://public/default/red_output"]
   parallelism: 1
   configs:
     projectId: "viva-magenta"
     topicId: "pantone"
     batchSize: 10
   ```
5. Run a Pulsar IO sink connector locally using [Pulsar CLI Tools `localrun`][1].
   ```shell
   $ bin/pulsar-admin sinks localrun \
     --sink-config-file examples/tz-pubsub-sink.yaml  \
     --archive examples/tz-pulsar-io-1.0-SNAPSHOT.nar
   
   $ bin/pulsar-admin sinks status --tenant public --namespace default --name pubsub-sink
   
   # if the sink already exists
   $ bin/pulsar-admin sinks list --tenant public --namespace default
   $ bin/pulsar-admin sinks stop --tenant public --namespace default --name pubsub-sink
   $ bin/pulsar-admin sinks start --tenant public --namespace default --name pubsub-sink
   ```
   
6. In a second terminal, produce messages to the Pulsar topic.
   ```shell
   $ bin/pulsar-client produce red_input -m "test-msg-`date`" -n 10
   ```
7. In a third terminal, pull messages from a Pub/Sub subscription.
   ```shell
   $ gcloud pubsub subscriptions pull pantone --auto-ack --limit=5
   ```
8. Cleanup.
   ```shell
    # stop sink connector
   $ bin/pulsar-admin sinks stop --tenant public --namespace default --name pubsub-sink
   
   # delete sink connector
   $ bin/pulsar-admin sinks delete --tenant public --namespace default --name pubsub-sink
   
   # delete topics
   $ bin/pulsar-admin topics delete persistent://public/default/${TOPIC_ID}
   
   # stop Docker container
   $ docker stop ${CONTAINER_ID}
   
   # delete Pub/Sub topic and subscription
   $ gcloud pubsub subscriptions delete ${SUBSCRIPTION_ID}
   $ gcloud pubsub topics delete ${TOPIC_ID}
   ```

## How to run the source connector

1. Prepare a Pub/Sub topic and subscription. The following examples make use of `projects/viva-magenta/topics/pantone` and `projects/viva-magenta/subscriptions/pantone`.
2. Package the Pub/Sub source connector with `mvn clean package -DskipTest=True`. Obtain `target/tz-pulsar-io-1.0-SNAPSHOT.nar`. Check out the contents with `jar xvf target/tz-pulsar-io-1.0-SNAPSHOT.nar`. Look out for:
   ```text
   org/apache/pulsar/io/gcp/PubsubConfig.class
   org/apache/pulsar/io/gcp/PubsubSource.class
   ```
3. Start a Pulsar cluster on Docker.
   ```shell
   # first time
   $ docker run -it -p 6650:6650 -p 8080:8080 --mount source=pulsardata,target=/pulsar/data --mount source=pulsarconf,target=/pulsar/conf apachepulsar/pulsar:2.11.1 bin/pulsar standalone

   # later times
   $ docker ps -a
   $ docker start ${CONTAINTER_ID}
   ```
4. Prepare the source config keys and values. Check out [`tz-pubsub-source.yaml`](src/test/resources/tz-pubsub-source.yaml).
   **Note: Although Pulsar CLI Tools uses `--destination-topic-name` in the source configs, the config YAML file expects `topicName`.**
   ```shell
   $ cat examples/tz-pubsub-source.yaml 
   tenant: "public"
   namespace: "default"
   name: "pubsub-source"
   topicName: "persistent://public/default/red_input"
   parallelism: 1
   configs:
     projectId: "viva-magenta"
     subscriptionId: "pantone"
     flowSize: 100
   ```
5. Run a Pulsar IO source connector locally using [Pulsar CLI Tools `localrun`][1].
   ```shell
   $ bin/pulsar-admin sources localrun \
     --source-config-file examples/tz-pubsub-source.yaml  \
     --archive examples/tz-pulsar-io-1.0-SNAPSHOT.nar \
     --destination-topic-name red_input
   
   $ bin/pulsar-admin sources status --tenant public --namespace default --name pubsub-source
   
   # if the source already exists
   $ bin/pulsar-admin sources list --tenant public --namespace default
   $ bin/pulsar-admin sources stop --tenant public --namespace default --name pubsub-source
   $ bin/pulsar-admin sources start --tenant public --namespace default --name pubsub-source
   ```

6. In a second terminal, produce messages to the Pub/Sub topic.
   ```shell
   $ gcloud pubsub topics publish pantone --message abc
   ```
7. In a third terminal, pull messages from the Pulsar topic.
   ```shell
   $ bin/pulsar-client consume red_input -s red_input_sub -n 0
   ```
8. Cleanup.
   ```shell
    # stop source connector
   $ bin/pulsar-admin sources stop --tenant public --namespace default --name pubsub-source
   
   # delete source connector
   $ bin/pulsar-admin sources delete --tenant public --namespace default --name pubsub-source
   
   # delete topics
   $ bin/pulsar-admin topics delete persistent://public/default/${TOPIC_ID}
   
   # stop Docker container
   $ docker stop ${CONTAINER_ID}
   
   # delete Pub/Sub topic and subscription
   $ gcloud pubsub subscriptions delete ${SUBSCRIPTION_ID}
   $ gcloud pubsub topics delete ${TOPIC_ID}
   ```

## Cloud Build

Set up [Cloud Build][2] to run integration tests. 

In this repo, each commit to the main branch [triggers][3] a run. [`Dockerfile`](Dockerfile) pulls a Docker image with Maven and Java 17 and install Apache Pulsar. [`cloudbuild.yaml`](cloudbuild.yaml) starts a Pulsar standalone cluster and runs the integration tests.

![img](Screenshot%202023-05-11%20at%203.16.13%20PM.png)

[1]: https://pulsar.apache.org/reference/#/2.11.x/pulsar-admin/sinks?id=localrun
[2]: https://cloud.google.com/build/docs/automating-builds/github/build-repos-from-github
[3]: https://cloud.google.com/build/docs/automating-builds/create-manage-triggers