# Kafka: Producer, Tracker, Partitions, and Retries

This Spring Boot and Kafka example has a **producer** that receives and publishes order events, and a **tracker** that consumes and processes them. Failed events are retried before being sent to an error queue.

## Architecture

![Order, retry, and error-queue flow diagram](https://raw.githubusercontent.com/Oswe-gif/kafka-project/97b75ed/docs/architecture.svg)

1. A client sends an order to the **producer** on port `8081` through `POST /api/messages`.
2. The producer publishes an `OrderCreatedEvent` as JSON to the Kafka `orders` topic, using `orderId` as the message key.
3. The **tracker**, available on port `8082`, consumes `orders` as part of the `group-1` consumer group and runs the business logic.
4. A successfully processed order completes the flow. A failed order moves through retry topics and finally to `orders.dlt`.

## Start the environment

### Prerequisites

- Docker Desktop (or Docker Engine with Docker Compose)
- Java 21

From the repository root, start Kafka:

```powershell
docker compose up -d
docker compose ps
```

Kafka is exposed at `localhost:9092`. Then start both Spring Boot services locally: **tracker** listens on port `8082` and **producer** on port `8081`.

To stop Kafka:

```powershell
docker compose down
```

> This command preserves the Kafka volume. Use `docker compose down -v` only when you also want to remove persisted messages.

## `orders` topic: partitions, not replicas

For the demonstration, the `orders` topic was **manually** expanded to three partitions only to observe how Kafka distributes events by key:

```powershell
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --alter --topic orders --partitions 3
```

This does **not** add data replicas. The topic details show `PartitionCount: 3`, but also `ReplicationFactor: 1`, `Replicas: 1`, and `Isr: 1` for every partition. There is a single broker, so each partition has one copy and there is no broker-failure tolerance.

```powershell
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
```

![orders topic details: three partitions and replication factor one](docs/images/orders-topic-partitions.png)

Partitions distribute work and preserve ordering within a partition. Real redundancy would require multiple brokers and a replication factor greater than one; that is outside the scope of this exercise.

## Same key, same partition

The producer uses `orderId` as the Kafka key:

```java
kafkaTemplate.send(topic, event.orderId().toString(), event)
```

When several orders use the same `orderId`, they have the same key. Kafka calculates the same partition for that key—as long as the partition count does not change—so it preserves the order of those events. In the test, the fixed ID `4b712433-3347-4411-ae2e-0e5c2822e284` was repeatedly published to partition `1`.

![Request with a fixed orderId](docs/images/request-fixed-order-id.png)

![The producer confirms messages with the same key reached partition 1](docs/images/producer-fixed-key-partition.png)

When `orderId` is omitted, as shown commented out in the next request, the producer generates a different UUID for each order. Because the key changes, Kafka can route the message to another partition; this run showed messages in partitions `1` and `2`. It does not have to rotate strictly: the exact partition depends on each UUID's hash.

![Request without orderId: the producer generates one](docs/images/request-generated-order-id.png)

![Generated keys result in messages across more than one partition](docs/images/producer-generated-key-partitions.png)

## View messages and processing results

Read all events stored in the main topic:

```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning
```

![Events stored in the orders topic](docs/images/orders-topic-messages.png)

To also see the partition, offset, and key of each event:

```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning --property print.partition=true --property print.offset=true --property print.key=true
```

![orders events with partition, offset, and key](docs/images/orders-partition-key-offset.png)

A valid order receives `202 Accepted` from the producer. The producer prints `Message sent to the partition: …`, and the tracker logs that it processed the event. The message remains in `orders` until Kafka's retention policy expires; consuming it does not delete it.

## Retries and Dead Letter Topic

In this project, an order whose `itemName` is `sushi` deliberately fails. The tracker detects that business rule and throws `Sushi orders are not supported`.

`@RetryableTopic(attempts = "4")` provides four total attempts: the initial attempt plus three retries. Spring Kafka publishes retries to separate topics so normal `orders` consumption is not blocked:

| Attempt | Observed topic | Delay before the next attempt |
| --- | --- | --- |
| 1 | `orders` | 1 second |
| 2 | `orders-retry-1000` | 2 seconds |
| 3 | `orders-retry-2000` | 4 seconds |
| 4 | `orders-retry-4000` | Sent to the DLT if it fails again |

The diagram uses abbreviated names for the three retry queues; the table contains the names observed at runtime, which include the delay in milliseconds.

This is why a `sushi` order produces four error logs: one for each processing attempt. The screenshot shows the 1-, 2-, and 4-second retry flow and the final publication of the failed event.

![Tracker logs while retrying a sushi order](docs/images/tracker-sushi-retries.png)

After all attempts are exhausted, Spring Kafka publishes a copy of the event to `orders.dlt`. The DLT keeps these events according to Kafka's retention policy so they can be inspected, fixed, and reprocessed in a controlled way. Verify it with:

```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders.dlt --from-beginning
```

![Sushi orders stored in orders.dlt](docs/images/orders-dlt-messages.png)

Retry topics do not remove the original message; they are additional steps in the flow. `orders` and the retry/DLT topics keep their records until Kafka applies its retention policy.

## Why is the DLT in the consumer instead of the producer?

A Dead Letter Topic represents an event that **reached Kafka successfully**, but that a consumer could not process after exhausting retries. Only the tracker can determine whether the business logic succeeded; for example, it is the component that rejects `sushi`.

The producer's responsibility is to publish the event. If its `KafkaTemplate` fails, the issue is publication or connectivity and the message may never have reached Kafka; sending it to a consumer DLT would be incorrect. Those cases require publication retries, alerts, or reliable storage for pending events.

The consumer can also add diagnostic metadata to the DLT, including the topic, partition, offset, and exception. That is why `@RetryableTopic` and `@DltHandler` are in `TrackerKafkaConsumer`.

## Improvement opportunity: Transactional Outbox

The producer currently publishes directly to Kafka. If it later stores the order in a database too, an inconsistency can occur: the order is saved but event publishing fails, or the event is published but saving the order fails.

The **Transactional Outbox** pattern prevents this:

1. Store the order and a pending `outbox` event record in the same database transaction.
2. Have a separate publisher read pending events and send them to Kafka.
3. Mark the event record as sent only after publication succeeds.

This provides reliable event delivery. The consumer must remain idempotent, since the publisher can resend an event if it fails immediately after publishing. The Outbox complements the DLT: it protects producer publication, while the DLT retains consumer-processing failures.

## Resources

- [Kafka resource playlist](https://www.youtube.com/playlist?list=PLRWubtXJnfRQ)
