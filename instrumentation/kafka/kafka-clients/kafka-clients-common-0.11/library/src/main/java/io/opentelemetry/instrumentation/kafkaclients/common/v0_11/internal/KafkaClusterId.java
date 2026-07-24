/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal;

import io.opentelemetry.api.common.AttributeKey;

/** Constants for the Kafka cluster-id span attribute. */
final class KafkaClusterId {

  static final AttributeKey<String> ATTRIBUTE_KEY =
      AttributeKey.stringKey("messaging.kafka.cluster.id");

  private KafkaClusterId() {}
}
