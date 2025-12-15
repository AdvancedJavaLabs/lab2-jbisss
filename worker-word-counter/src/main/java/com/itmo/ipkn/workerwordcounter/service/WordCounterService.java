package com.itmo.ipkn.workerwordcounter.service;

import com.itmo.ipkn.workerwordcounter.rabbit.listener.WordCounterDto;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class WordCounterService {

    private final RabbitTemplate rabbitTemplate;

    public WordCounterService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void countWordInTextAndSendToAggregator(WordCounterDto dto) {
        int wordCount = Arrays.stream(dto.getTextLine().split("[^\\p{L}\\p{Nd}]+")).toList().stream()
                .filter(wordInText -> wordInText.equalsIgnoreCase(dto.getWordToCount()))
                .toList().size();
        System.out.println("Handling message: " + dto.getMessageId());
        String lastMessage = dto.getLastMessage();
        if (lastMessage.equals("last")) {
            rabbitTemplate.convertAndSend("wordCounterOutput", dto.getTaskId() + "|" + dto.getMessageId() + "|" + wordCount
                    + "|" + dto.getWordToCount() +"|" + dto.getLastMessage() + "|" + System.currentTimeMillis());
        } else if (lastMessage.equals("first")) {
            rabbitTemplate.convertAndSend("wordCounterOutput", dto.getTaskId() + "|" + dto.getMessageId() + "|" + wordCount
                    + "|" + dto.getWordToCount() +"|" + dto.getLastMessage() + "|" + System.currentTimeMillis());
        } else {
            rabbitTemplate.convertAndSend("wordCounterOutput", dto.getTaskId() + "|" + dto.getMessageId() + "|" + wordCount
                    + "|" + dto.getWordToCount() +"|" + dto.getLastMessage());
        }
    }
}
