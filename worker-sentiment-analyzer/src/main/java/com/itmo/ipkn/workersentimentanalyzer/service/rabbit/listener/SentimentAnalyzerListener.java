package com.itmo.ipkn.workersentimentanalyzer.service.rabbit.listener;

import com.itmo.ipkn.workersentimentanalyzer.service.SentimentAnalyzerService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class SentimentAnalyzerListener {

    private final SentimentAnalyzerService sentimentAnalyzerService;

    public SentimentAnalyzerListener(SentimentAnalyzerService sentimentAnalyzerService) {
        this.sentimentAnalyzerService = sentimentAnalyzerService;
    }

    @RabbitListener(queues = "sentimentAnalyzerQueue")
    public void processMessage(String message) {
        sentimentAnalyzerService.analyzeMessageAndSendToAggregator(message);
    }
}
