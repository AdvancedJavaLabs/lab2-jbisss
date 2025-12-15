package com.itmo.ipkn.workerwordcounter.rabbit.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itmo.ipkn.workerwordcounter.service.WordCounterService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class WordCounterRabbitListener {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final WordCounterService wordCounterService;

    public WordCounterRabbitListener(WordCounterService wordCounterService) {
        this.wordCounterService = wordCounterService;
    }

    @RabbitListener(queues = "wordCounterQueue")
    public void processMessage(String message) {
        WordCounterDto dto;
        try {
            dto = objectMapper.readValue(message, WordCounterDto.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        if (dto != null) {
            wordCounterService.countWordInTextAndSendToAggregator(dto);
        }
    }
}
