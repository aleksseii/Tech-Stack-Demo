package ru.aleksseii;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.aleksseii.event.Event;

@Slf4j
@Component
public class KafkaEventConsumer {

    @KafkaListener(containerFactory = "events",
            topics = "${kafka.events.topic}",
            groupId = "${kafka.events.group-id}",
            autoStartup = "true")
    public void consume(Event event) {
        log.info("Processed event {}", event);
    }
}
