/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal;

import static java.util.Collections.emptyMap;
import static java.util.logging.Level.FINE;
import static java.util.stream.Collectors.joining;

import io.opentelemetry.instrumentation.api.util.VirtualField;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

/**
 * This class is internal and is hence not for public use. Its APIs are unstable and can change at
 * any time.
 */
public final class KafkaUtil {

  private static final Logger logger = Logger.getLogger(KafkaUtil.class.getName());

  private static final String CONSUMER_GROUP = "consumer_group";
  private static final String CLIENT_ID = "client_id";

  private static final VirtualField<Consumer<?, ?>, Map<String, String>> consumerInfoField =
      VirtualField.find(Consumer.class, Map.class);

  // Cached per-instance. Metadata is a kafka-clients application class (same classloader as
  // Consumer), so it is safe to use as the VirtualField value type across instrumentation modules.
  private static final VirtualField<Consumer<?, ?>, Metadata> consumerMetadataField =
      VirtualField.find(Consumer.class, Metadata.class);

  // ClassValue caches the reflective Field per class. computeValue() runs at most once per class —
  // thread-safe and GC-friendly (entry released when the ClassLoader is collected).
  // Optional.empty() is stored when the field is absent or inaccessible, preventing repeated
  // lookups.
  private static final ClassValue<Optional<Field>> metadataFieldCache = buildFieldCache("metadata");

  // Kafka 3.7+ wraps metadata inside a LegacyKafkaConsumer delegate.
  private static final ClassValue<Optional<Field>> delegateFieldCache = buildFieldCache("delegate");

  private static final Set<String> reflectionFailuresLogged = ConcurrentHashMap.newKeySet();

  private static final MethodHandle GET_GROUP_METADATA;
  private static final MethodHandle GET_GROUP_ID;
  private static final Field PRODUCER_CONFIG_FIELD;

  static {
    MethodHandle getGroupMetadata;
    MethodHandle getGroupId;
    Field producerConfigField;

    try {
      Class<?> consumerGroupMetadata =
          Class.forName("org.apache.kafka.clients.consumer.ConsumerGroupMetadata");

      // Consumer.groupMetadata() and ConsumerGroupMetadata exist only in Kafka 2.4+. Using
      // Class.forName + MethodHandles (rather than direct calls) lets this file compile against
      // the Kafka 0.11 baseline; ClassNotFoundException/NoSuchMethodException in the catch block
      // sets GET_GROUP_METADATA to null so consumer-group extraction is silently skipped on
      // older Kafka versions.
      MethodHandles.Lookup lookup = MethodHandles.publicLookup();
      getGroupMetadata =
          lookup.findVirtual(
              Consumer.class, "groupMetadata", MethodType.methodType(consumerGroupMetadata));
      getGroupId =
          lookup.findVirtual(consumerGroupMetadata, "groupId", MethodType.methodType(String.class));

      producerConfigField = KafkaProducer.class.getDeclaredField("producerConfig");
      producerConfigField.setAccessible(true);
    } catch (ClassNotFoundException
        | IllegalAccessException
        | NoSuchMethodException
        | NoSuchFieldException ignored) {
      getGroupMetadata = null;
      getGroupId = null;
      producerConfigField = null;
    }

    GET_GROUP_METADATA = getGroupMetadata;
    GET_GROUP_ID = getGroupId;
    PRODUCER_CONFIG_FIELD = producerConfigField;
  }

  @Nullable
  public static String getConsumerGroup(@Nullable Consumer<?, ?> consumer) {
    return getConsumerInfo(consumer).get(CONSUMER_GROUP);
  }

  @Nullable
  public static String getClientId(@Nullable Consumer<?, ?> consumer) {
    return getConsumerInfo(consumer).get(CLIENT_ID);
  }

  private static Map<String, String> getConsumerInfo(@Nullable Consumer<?, ?> consumer) {
    if (consumer == null) {
      return emptyMap();
    }
    Map<String, String> map = consumerInfoField.get(consumer);
    if (map == null) {
      map = new HashMap<>();
      map.put(CONSUMER_GROUP, extractConsumerGroup(consumer));
      map.put(CLIENT_ID, extractClientId(consumer));
      consumerInfoField.set(consumer, map);
    }
    return map;
  }

  @Nullable
  private static String extractConsumerGroup(Consumer<?, ?> consumer) {
    if (GET_GROUP_METADATA == null || GET_GROUP_ID == null) {
      return null;
    }
    try {
      Object metadata = GET_GROUP_METADATA.invoke(consumer);
      return (String) GET_GROUP_ID.invoke(metadata);
    } catch (Throwable ignored) {
      return null;
    }
  }

  @Nullable
  private static String extractClientId(Consumer<?, ?> consumer) {
    try {
      Map<MetricName, ? extends Metric> metrics = consumer.metrics();
      Iterator<MetricName> metricIterator = metrics.keySet().iterator();
      return metricIterator.hasNext() ? metricIterator.next().tags().get("client-id") : null;
    } catch (RuntimeException ignored) {
      // ExceptionHandlingTest uses a Consumer that throws exception on every method call
      return null;
    }
  }

  @Nullable
  public static String extractBootstrapServers(Producer<?, ?> producer) {
    if (PRODUCER_CONFIG_FIELD == null || !KafkaProducer.class.equals(producer.getClass())) {
      return null;
    }
    try {
      ProducerConfig producerConfig = (ProducerConfig) PRODUCER_CONFIG_FIELD.get(producer);
      return extractBootstrapServers(
          producerConfig.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    } catch (IllegalAccessException | IllegalArgumentException ignored) {
      return null;
    }
  }

  @Nullable
  public static String extractBootstrapServers(@Nullable List<String> serversConfig) {
    if (serversConfig == null) {
      return null;
    }
    return serversConfig.stream().map(Object::toString).collect(joining(","));
  }

  /** Reads cluster id from the VirtualField cached by {@link #cacheConsumerMetadata}. */
  @Nullable
  public static String getClusterId(@Nullable Consumer<?, ?> consumer) {
    if (consumer == null) {
      return null;
    }
    return clusterIdFromMetadata(consumerMetadataField.get(consumer));
  }

  /**
   * Proactively caches the live {@code Metadata} reference on the consumer's VirtualField so that
   * {@link #getClusterId} needs no per-span reflection. No-op if already populated.
   *
   * <p>Supports both pre-3.7 Kafka (metadata field on {@code KafkaConsumer}) and Kafka 3.7+
   * (metadata field on the internal {@code LegacyKafkaConsumer} delegate). Reflection per class is
   * performed at most once via {@code ClassValue}.
   */
  public static void cacheConsumerMetadata(Consumer<?, ?> consumer) {
    if (consumerMetadataField.get(consumer) != null) {
      return;
    }
    // Pre-3.7: metadata field lives directly on KafkaConsumer.
    Metadata metadata = extractMetadataFromHolder(consumer);
    if (metadata == null) {
      // Kafka 3.7+: KafkaConsumer wraps a LegacyKafkaConsumer delegate that holds metadata.
      metadata = extractMetadataFromHolder(extractDelegate(consumer));
    }
    if (metadata != null) {
      consumerMetadataField.set(consumer, metadata);
    }
  }

  /**
   * Reads the {@code metadata} field from {@code holder} via one-time-per-class reflection.
   * Package-private for unit testing.
   */
  @Nullable
  static Metadata extractMetadataFromHolder(@Nullable Object holder) {
    if (holder == null) {
      return null;
    }
    Field field = metadataFieldCache.get(holder.getClass()).orElse(null);
    if (field == null) {
      return null;
    }
    try {
      return (Metadata) field.get(holder);
    } catch (IllegalAccessException | ClassCastException e) {
      logReflectionFailureOnce(holder.getClass(), e.toString());
      return null;
    }
  }

  @Nullable
  private static Object extractDelegate(@Nullable Object consumer) {
    if (consumer == null) {
      return null;
    }
    Field field = delegateFieldCache.get(consumer.getClass()).orElse(null);
    if (field == null) {
      return null;
    }
    try {
      return field.get(consumer);
    } catch (IllegalAccessException e) {
      logReflectionFailureOnce(consumer.getClass(), e.toString());
      return null;
    }
  }

  @Nullable
  public static String clusterIdFromMetadata(@Nullable Metadata metadata) {
    if (metadata == null) {
      return null;
    }
    try {
      Cluster cluster = metadata.fetch();
      if (cluster == null) {
        return null;
      }
      ClusterResource resource = cluster.clusterResource();
      if (resource == null) {
        return null;
      }
      String id = resource.clusterId();
      return (id != null && !id.isEmpty()) ? id : null;
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  private static ClassValue<Optional<Field>> buildFieldCache(String fieldName) {
    return new ClassValue<Optional<Field>>() {
      @Override
      protected Optional<Field> computeValue(Class<?> holderClass) {
        for (Class<?> c = holderClass; c != null; c = c.getSuperclass()) {
          try {
            Field field = c.getDeclaredField(fieldName);
            try {
              field.setAccessible(true);
            } catch (RuntimeException e) {
              logReflectionFailureOnce(holderClass, e.toString());
              return Optional.empty();
            }
            return Optional.of(field);
          } catch (NoSuchFieldException ignored) {
            // not on this class; try superclass
          }
        }
        return Optional.empty();
      }
    };
  }

  private static void logReflectionFailureOnce(Class<?> holderClass, String detail) {
    if (logger.isLoggable(FINE) && reflectionFailuresLogged.add(holderClass.getName())) {
      logger.log(
          FINE,
          "Unable to resolve Kafka cluster id from {0}: {1}. messaging.kafka.cluster.id will be"
              + " absent for this client type.",
          new Object[] {holderClass.getName(), detail});
    }
  }

  private KafkaUtil() {}
}
