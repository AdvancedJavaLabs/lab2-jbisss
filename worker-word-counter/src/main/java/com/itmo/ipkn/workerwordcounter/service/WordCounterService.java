package com.itmo.ipkn.workerwordcounter.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class WordCounterService {

    private final RabbitTemplate rabbitTemplate;

    private int counter = 0;

    public WordCounterService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void countWordInTextAndSendToAggregator(String text, String word) {
        int wordCount = Arrays.stream(text.split("[^\\p{L}\\p{Nd}]+")).toList().stream()
                .filter(wordInText -> wordInText.equals(word))
                .toList().size();
        counter+=wordCount;
        System.out.println(counter);
        // rabbitTemplate.convertAndSend("wordCounterOutput", wordCount);
    }
}
