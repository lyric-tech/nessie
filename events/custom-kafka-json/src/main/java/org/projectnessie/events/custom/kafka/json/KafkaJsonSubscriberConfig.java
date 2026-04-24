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

import io.smallrye.config.ConfigMapping;
import java.util.Optional;
import java.util.Set;
import org.projectnessie.events.api.EventType;

/** Configuration for the custom Kafka JSON event subscriber. */
@ConfigMapping(prefix = "nessie.events.subscribers.kafka-json")
public interface KafkaJsonSubscriberConfig {

  /** Whether this subscriber is enabled. Defaults to false. */
  Optional<Boolean> enabled();

  /** The repository IDs to watch. If empty, all repositories are watched. */
  Optional<Set<String>> repositoryIds();

  /** The event types to watch. If empty, all event types are watched. */
  Optional<Set<EventType>> eventTypes();
}
