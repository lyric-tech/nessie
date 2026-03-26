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

plugins {
  alias(libs.plugins.quarkus)
  id("nessie-conventions-quarkus")
}

extra["maven.name"] = "Nessie - Events - Custom Kafka JSON Subscriber"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-events-api"))
  implementation(project(":nessie-events-spi"))

  // Quarkus
  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-core")

  // Quarkus - Kafka
  implementation("io.quarkus:quarkus-messaging-kafka")

  // Jackson serialization
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")

  compileOnly(libs.microprofile.openapi)
}

listOf("javadoc", "sourcesJar").forEach { name ->
  tasks.named(name).configure { dependsOn("compileQuarkusGeneratedSourcesJava") }
}

listOf("checkstyleTest", "compileTestJava").forEach { name ->
  tasks.named(name).configure { dependsOn("compileQuarkusTestGeneratedSourcesJava") }
}

tasks.named("quarkusDependenciesBuild").configure { dependsOn("processJandexIndex") }
