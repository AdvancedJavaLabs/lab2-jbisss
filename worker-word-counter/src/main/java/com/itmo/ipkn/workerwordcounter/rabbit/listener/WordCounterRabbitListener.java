package com.itmo.ipkn.workerwordcounter.rabbit.listener;

import com.itmo.ipkn.workerwordcounter.service.WordCounterService;
import org.springframework.stereotype.Service;

@Service
public class WordCounterRabbitListener {

    private final WordCounterService wordCounterService;

    public WordCounterRabbitListener(WordCounterService wordCounterService) {
        this.wordCounterService = wordCounterService;
    }

    public void listen() {

    }
}
