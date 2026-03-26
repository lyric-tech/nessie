/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.events.custom.kafka.json;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.api.MultiReferenceEvent;
import org.projectnessie.events.api.ReferenceEvent;
import org.projectnessie.events.spi.EventFilter;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.events.spi.EventTypeFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom {@link EventSubscriber} that publishes Nessie events to a Kafka topic as JSON.
 *
 * <p>This is a standalone implementation that depends only on the Nessie events SPI and SmallRye
 * Kafka. It does not extend the RI's AbstractMessagingEventSubscriber, avoiding NATS and Avro
 * dependencies entirely.
 */
@ApplicationScoped
@LookupIfProperty(name = "nessie.events.subscribers.kafka-json.enabled", stringValue = "true")
public class KafkaJsonEventSubscriber implements EventSubscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonEventSubscriber.class);

  static final String CHANNEL = "nessie-kafka-json";

  // Header keys
  static final String HEADER_EVENT_ID = "event-id";
  static final String HEADER_SPEC_VERSION = "spec-version";
  static final String HEADER_API_VERSION = "api-version";
  static final String HEADER_EVENT_TYPE = "event-type";
  static final String HEADER_REPOSITORY_ID = "repository-id";
  static final String HEADER_INITIATOR = "initiator";
  static final String HEADER_EVENT_CREATION_TIME = "event-creation-time";
  static final String HEADER_COMMIT_CREATION_TIME = "commit-creation-time";

  private final Emitter<Event> emitter;
  private final KafkaJsonSubscriberConfig config;

  // Initialized with safe defaults so that accepts() works before onSubscribe() runs.
  // The EventSubscribers constructor calls accepts() during bean construction to build
  // its event-type bitmask. These defaults ensure no NPE and "accept all" semantics
  // until onSubscribe() narrows them from configuration.
  private EventTypeFilter eventTypeFilter = EventTypeFilter.all();
  private EventFilter eventFilter = EventFilter.all();

  private String serverSpecVersion;
  private String apiVersion;

  /** No-arg constructor required by CDI. */
  @SuppressWarnings("unused")
  public KafkaJsonEventSubscriber() {
    this.emitter = null;
    this.config = null;
  }

  @Inject
  public KafkaJsonEventSubscriber(
      @Channel(CHANNEL) Emitter<Event> emitter, KafkaJsonSubscriberConfig config) {
    this.emitter = emitter;
    this.config = config;
  }

  @Override
  public boolean isBlocking() {
    return false;
  }

  @Override
  public void onSubscribe(EventSubscription subscription) {
    serverSpecVersion = subscription.getSystemConfiguration().getSpecVersion();
    apiVersion = String.valueOf(subscription.getSystemConfiguration().getMaxSupportedApiVersion());

    if (config != null) {
      Set<String> repoIds = config.repositoryIds().orElse(Set.of());
      eventFilter =
          repoIds.isEmpty() ? EventFilter.all() : e -> repoIds.contains(e.getRepositoryId());

      Set<EventType> eventTypes = config.eventTypes().orElse(Set.of());
      eventTypeFilter =
          eventTypes.isEmpty() ? EventTypeFilter.all() : EventTypeFilter.of(eventTypes);
    }

    LOGGER.info("Kafka JSON event subscriber started, subscription id: {}", subscription.getId());
  }

  @Override
  public EventTypeFilter getEventTypeFilter() {
    return eventTypeFilter;
  }

  @Override
  public EventFilter getEventFilter() {
    return eventFilter;
  }

  @Override
  public void onEvent(Event event) {
    try {
      RecordHeaders headers = new RecordHeaders();
      addHeaders(event, headers);

      OutgoingKafkaRecordMetadata<String> metadata =
          OutgoingKafkaRecordMetadata.<String>builder()
              .withKey(recordKey(event))
              .withHeaders(headers)
              .build();

      Message<Event> message =
          Message.of(event, Metadata.of(metadata))
              .withAckWithMetadata(
                  m -> {
                    if (LOGGER.isDebugEnabled()) {
                      LOGGER.debug(
                          "Event written to Kafka: id={}, type={}",
                          event.getIdAsText(),
                          event.getType());
                    }
                    return CompletableFuture.completedFuture(null);
                  })
              .withNackWithMetadata(
                  (error, m) -> {
                    LOGGER.error(
                        "Failed to write event to Kafka: id={}, type={}",
                        event.getIdAsText(),
                        event.getType(),
                        error);
                    return CompletableFuture.completedFuture(null);
                  });

      emitter.send(message);
    } catch (RuntimeException e) {
      LOGGER.error("Failed to send event: id={}", event.getIdAsText(), e);
      throw e;
    }
  }

  @Override
  public void close() {
    if (emitter != null) {
      emitter.complete();
    }
    LOGGER.info("Kafka JSON event subscriber closed");
  }

  private void addHeaders(Event event, RecordHeaders headers) {
    addHeader(headers, HEADER_EVENT_ID, event.getIdAsText());
    addHeader(headers, HEADER_SPEC_VERSION, serverSpecVersion);
    addHeader(headers, HEADER_API_VERSION, apiVersion);
    addHeader(headers, HEADER_EVENT_TYPE, event.getType().name());
    addHeader(headers, HEADER_REPOSITORY_ID, event.getRepositoryId());
    event.getEventInitiator().ifPresent(user -> addHeader(headers, HEADER_INITIATOR, user));
    addHeader(headers, HEADER_EVENT_CREATION_TIME, event.getEventCreationTimestamp().toString());

    if (event instanceof CommitEvent commitEvent) {
      Instant commitTime = Objects.requireNonNull(commitEvent.getCommitMeta().getCommitTime());
      addHeader(headers, HEADER_COMMIT_CREATION_TIME, commitTime.toString());
    } else if (event instanceof ContentEvent contentEvent) {
      Instant commitTime = contentEvent.getCommitCreationTimestamp();
      addHeader(headers, HEADER_COMMIT_CREATION_TIME, commitTime.toString());
    }
  }

  private static void addHeader(RecordHeaders headers, String key, String value) {
    headers.add(key, value.getBytes(UTF_8));
  }

  /**
   * Record key combining repository ID and reference name. This ensures events for the same
   * reference land in the same Kafka partition, preserving ordering per-reference.
   */
  static String recordKey(Event event) {
    String repositoryId = event.getRepositoryId();
    String reference;
    if (event instanceof ReferenceEvent refEvent) {
      reference = refEvent.getReference().getName();
    } else if (event instanceof MultiReferenceEvent multiRefEvent) {
      reference = multiRefEvent.getTargetReference().getName();
    } else {
      reference = "unknown";
    }
    return repositoryId + ":" + reference;
  }
}
