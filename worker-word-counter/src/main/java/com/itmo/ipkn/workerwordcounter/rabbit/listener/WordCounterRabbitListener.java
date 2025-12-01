package com.itmo.ipkn.workerwordcounter.rabbit.listener;

import com.itmo.ipkn.workerwordcounter.service.WordCounterService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class WordCounterRabbitListener {

    private final WordCounterService wordCounterService;

    public WordCounterRabbitListener(WordCounterService wordCounterService) {
        this.wordCounterService = wordCounterService;
    }

    @RabbitListener(queues = "wordCounterQueue")
    public void processMessage(String message) {
        String[] tokens = message.split("\\|");
        wordCounterService.countWordInTextAndSendToAggregator(tokens.length < 2 ? "" : tokens[1], tokens[0]);
    }
}
