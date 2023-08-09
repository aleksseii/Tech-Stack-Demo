package ru.aleksseii;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.aleksseii.event.Event;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventProducer {

    private static long CURRENT_EVENT_ID = 0;

    private static final String[] EVENT_TYPES = new String[]{"STANDARD", "NEW", "OLD"};

    @Value("${kafka.events.topic}")
    private String topic;

    private final KafkaTemplate<Long, Event> kafkaTemplate;

    @Scheduled(fixedRate = 3000)
    public void send() {
        Long key = ThreadLocalRandom.current().nextLong();
        long eventId = nextEventId();
        Event event = Event.newBuilder()
                .setId(eventId)
                .setName("Event #" + eventId)
                .setType(EVENT_TYPES[(int) eventId % EVENT_TYPES.length])
                .setDate(Instant.now())
                .build();
        kafkaTemplate
                .send(topic, key, event)
                .thenAccept(result -> log.info("Successfully sent event {} in topic {}, metadata: {}",
                        result.getProducerRecord(), topic, result.getRecordMetadata()));
    }

    private long nextEventId() {
        return ++CURRENT_EVENT_ID;
    }

}
