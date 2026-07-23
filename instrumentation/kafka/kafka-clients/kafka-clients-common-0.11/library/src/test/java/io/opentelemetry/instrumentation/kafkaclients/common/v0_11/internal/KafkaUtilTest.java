/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.junit.jupiter.api.Test;

class KafkaUtilTest {

  // --- clusterIdFromMetadata ---

  @Test
  void clusterIdFromMetadata_null_returnsNull() {
    assertThat(KafkaUtil.clusterIdFromMetadata(null)).isNull();
  }

  @Test
  void clusterIdFromMetadata_validId_returnsId() {
    Metadata metadata = mock(Metadata.class);
    Cluster cluster =
        new Cluster(
            "test-cluster",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptySet(),
            Collections.emptySet());
    when(metadata.fetch()).thenReturn(cluster);

    assertThat(KafkaUtil.clusterIdFromMetadata(metadata)).isEqualTo("test-cluster");
  }

  @Test
  void clusterIdFromMetadata_emptyId_returnsNull() {
    Metadata metadata = mock(Metadata.class);
    Cluster cluster =
        new Cluster(
            "",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptySet(),
            Collections.emptySet());
    when(metadata.fetch()).thenReturn(cluster);

    assertThat(KafkaUtil.clusterIdFromMetadata(metadata)).isNull();
  }

  @Test
  void clusterIdFromMetadata_fetchThrows_returnsNull() {
    Metadata metadata = mock(Metadata.class);
    when(metadata.fetch()).thenThrow(new RuntimeException("simulated fetch failure"));

    assertThat(KafkaUtil.clusterIdFromMetadata(metadata)).isNull();
  }

  // --- extractMetadataFromHolder ---

  @Test
  void extractMetadataFromHolder_null_returnsNull() {
    assertThat(KafkaUtil.extractMetadataFromHolder(null)).isNull();
  }

  @Test
  void extractMetadataFromHolder_noMetadataField_returnsNull() {
    // String has no "metadata" field — returns null without throwing
    assertThat(KafkaUtil.extractMetadataFromHolder("not a consumer")).isNull();
  }

  @Test
  void extractMetadataFromHolder_withMetadataField_returnsMetadata() {
    Metadata mockMetadata = mock(Metadata.class);
    HolderWithMetadata holder = new HolderWithMetadata(mockMetadata);

    assertThat(KafkaUtil.extractMetadataFromHolder(holder)).isSameAs(mockMetadata);
  }

  @Test
  void extractMetadataFromHolder_nullMetadataField_returnsNull() {
    // Field present but set to null — ClassCastException branch isn't hit; cast of null is null.
    // Actually field.get(holder) returns null, and (Metadata) null = null — no exception.
    HolderWithMetadata holder = new HolderWithMetadata(null);

    assertThat(KafkaUtil.extractMetadataFromHolder(holder)).isNull();
  }

  @Test
  void extractMetadataFromHolder_metadataOnSuperclass_returnsMetadata() {
    // Validates the superclass traversal in buildFieldCache: the "metadata" field is declared on
    // the parent class, not on the subclass — mirrors a user subclassing KafkaConsumer.
    Metadata mockMetadata = mock(Metadata.class);
    SubclassHolder holder = new SubclassHolder(mockMetadata);

    assertThat(KafkaUtil.extractMetadataFromHolder(holder)).isSameAs(mockMetadata);
  }

  // Mimics the structure of KafkaConsumer (has a "metadata" field of type Metadata).
  private static class HolderWithMetadata {
    @SuppressWarnings("unused")
    @Nullable
    private final Metadata metadata;

    HolderWithMetadata(@Nullable Metadata metadata) {
      this.metadata = metadata;
    }
  }

  // Mimics a user subclass of KafkaConsumer: "metadata" lives on the parent, not the subclass.
  private static final class SubclassHolder extends HolderWithMetadata {
    SubclassHolder(@Nullable Metadata metadata) {
      super(metadata);
    }
  }
}
