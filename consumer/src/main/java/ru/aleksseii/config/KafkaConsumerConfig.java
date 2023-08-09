package ru.aleksseii.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import ru.aleksseii.event.Event;

import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.schema-registry.url}")
    private String schemaRegistryUrl;
    @Value("${kafka.events.group-id}")
    private String groupId;

    @Bean("events")
    public ConcurrentKafkaListenerContainerFactory<Long, Event> eventKafkaContainerFactory() {
        var consumerFactory =  new DefaultKafkaConsumerFactory<>(getConsumerFactoryProperties());
        var listener = new ConcurrentKafkaListenerContainerFactory<Long, Event>();
        listener.setConcurrency(1);
        listener.setConsumerFactory(consumerFactory);
        return listener;
    }

    private Map<String, Object> getConsumerFactoryProperties() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true",
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 3000
        );
    }

}
