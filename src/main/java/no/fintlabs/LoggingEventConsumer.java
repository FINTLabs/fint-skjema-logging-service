package no.fintlabs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.logging.LogEvent;
import no.fintlabs.kafka.producer.OriginHeaderProducerInterceptor;
import no.fintlabs.kafka.topic.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.StringJoiner;

@Slf4j
@Component
public class LoggingEventConsumer {

    private final ObjectMapper objectMapper;

    public LoggingEventConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Bean
    @Qualifier("logTopic")
    TopicDescription logTopic(TopicService topicService) {
        return topicService.getOrCreateLoggingTopic();
    }

    @KafkaListener(topics = "#{logTopic.name()}")
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        String originApplicationId = new String(
                consumerRecord.headers().lastHeader(OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_RECORD_HEADER).value(),
                StandardCharsets.UTF_8
        );
        try {
            LogEvent logEvent = this.objectMapper.readValue(consumerRecord.value(), LogEvent.class);
            this.log(originApplicationId, logEvent);
        } catch (JsonProcessingException e) {
            log.error("Could not deserialize log event", e);
        }
    }

    private void log(String originApplicationId, LogEvent logEvent) {
        switch (logEvent.getLevel()) {
            case TRACE:
                log.trace(this.formatLogEvent(originApplicationId, logEvent), logEvent.getThrowable());
                break;
            case DEBUG:
                log.debug(this.formatLogEvent(originApplicationId, logEvent), logEvent.getThrowable());
                break;
            case INFO:
                log.info(this.formatLogEvent(originApplicationId, logEvent), logEvent.getThrowable());
                break;
            case WARN:
                log.warn(this.formatLogEvent(originApplicationId, logEvent), logEvent.getThrowable());
                break;
            case ERROR:
                log.error(this.formatLogEvent(originApplicationId, logEvent), logEvent.getThrowable());
                break;
        }
    }

    private String formatLogEvent(String originApplicationId, LogEvent logEvent) {
        return new StringJoiner(" - ")
                .add(LocalDateTime.ofInstant(Instant.ofEpochMilli(logEvent.getTimestamp()), ZoneId.systemDefault()).toString())
                .add(originApplicationId)
                .add(logEvent.getThreadName())
                .add(logEvent.getLoggerName())
                .add(logEvent.getMessage())
                .toString();
    }
}
