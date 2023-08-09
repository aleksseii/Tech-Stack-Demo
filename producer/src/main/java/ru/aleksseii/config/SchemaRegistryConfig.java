package ru.aleksseii.config;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import ru.aleksseii.event.Event;

@SuppressWarnings("SameParameterValue")
@Slf4j
public class SchemaRegistryConfig {

    @Value("${kafka.schema-registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.events.topic}")
    private String topic;

    private static final String FORWARD = "FORWARD";
    private static final String BACKWARD = "BACKWARD";

    @Bean
    @ConditionalOnProperty("kafka.schema-registry.required")
    public CachedSchemaRegistryClient schemaRegistryClient() {
        var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
        registerSchema(schemaRegistryClient, topic, Event.getClassSchema(), 1, FORWARD);
        return schemaRegistryClient;
    }

    private void registerSchema(
            CachedSchemaRegistryClient schemaRegistryClient,
            String topic,
            Schema classSchema,
            int version,
            String compatibility
    ) {
        var subject = topic + "-value";
        try {
            schemaRegistryClient.updateCompatibility(subject, compatibility);
            schemaRegistryClient.register(subject, new AvroSchema(classSchema), version, -1);
            log.info("AVRO schema {} successfully registered for topic {}.", classSchema, topic);
        } catch (Exception e) {
            log.error("Unable to register AVRO schema in Kafka: {}", classSchema, e);
            throw new RuntimeException("Unable to register AVRO schema in Kafka", e);
        }
    }

}
