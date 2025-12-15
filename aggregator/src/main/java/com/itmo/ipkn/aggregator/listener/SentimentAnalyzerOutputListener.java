package com.itmo.ipkn.aggregator.listener;

import com.itmo.ipkn.aggregator.aggregation.SentimentAnalyzerAggregator;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SentimentAnalyzerOutputListener {

    private final Map<Integer, SentimentAnalyzerAggregator> aggregatorMap = new ConcurrentHashMap<>();

    @RabbitListener(queues = "sentimentAnalyzerQueueOutput")
    public void processMessage(String message) {
        String[] tokens = message.split("\\|");

        int taskId = Integer.parseInt(tokens[0]);
        int messageId = Integer.parseInt(tokens[1]);
        double messageCoef = Double.parseDouble(tokens[2]);
        String messageType = tokens[3];
        String timeIfHas = tokens[4];

        SentimentAnalyzerAggregator aggregator;
        if (aggregatorMap.containsKey(taskId)) {
            aggregator = aggregatorMap.get(taskId);
        } else {
            aggregator = new SentimentAnalyzerAggregator(taskId);
            aggregatorMap.put(taskId, aggregator);
        }
        aggregator.handleMessage(messageId, messageCoef, messageType, timeIfHas);
    }
}
