package com.itmo.ipkn.workersentimentanalyzer.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SentimentAnalyzerService {

    private final RabbitTemplate rabbitTemplate;

    private final Map<String, Double> MAP = new HashMap<>();

    public SentimentAnalyzerService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;

        MAP.put("отец", 3.0);
        MAP.put("мать", 3.0);
        MAP.put("несчастного", -3.0);
    }

    public void analyzeMessageAndSendToAggregator(String message) {
        String[] tokens = message.split("\\|");

        int taskId = Integer.parseInt(tokens[0]);
        int messageId = Integer.parseInt(tokens[1]);
        String textLine = tokens[2];
        String messageType = tokens[3];
        String timeIfHas = tokens[4];

        double resultCoef = analyze(textLine);

        String outputMessage = taskId + "|" + messageId + "|" + resultCoef + "|" + messageType + "|" + timeIfHas;
        rabbitTemplate.convertAndSend("sentimentAnalyzerQueueOutput", outputMessage);
    }

    private double analyze(String text) {
        List<String> words = Arrays.stream(text.split("[^\\p{L}\\p{Nd}]+")).toList();
        double sum = 0;
        for (String word : words) {
            sum += MAP.getOrDefault(word.toLowerCase(), 0.0);
        }
        return !words.isEmpty() ? sum / words.size() : 0;
    }
}
