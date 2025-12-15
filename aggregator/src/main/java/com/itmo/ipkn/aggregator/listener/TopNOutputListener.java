package com.itmo.ipkn.aggregator.listener;

import com.itmo.ipkn.aggregator.aggregation.TopNAggregation;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TopNOutputListener {

    private final Map<Integer, TopNAggregation> mapTasksAggregations = new ConcurrentHashMap<>();

    @RabbitListener(queues = "topNQueueOutput")
    public void processMessage(String message) {
        String[] tokens = message.split("\\|");
        int tokensSize = tokens.length;

        String messageStatus = tokens[tokensSize - 1];
        int taskId = Integer.parseInt(tokens[tokensSize - 2]);
        int messageId = Integer.parseInt(tokens[tokensSize - 3]);
        String startTime = tokens[tokensSize - 4];
        int n = Integer.parseInt(tokens[tokensSize - 5]);

        TopNAggregation topNAggregation;
        if (mapTasksAggregations.containsKey(taskId)) {
            topNAggregation = mapTasksAggregations.get(taskId);
        } else {
            topNAggregation = new TopNAggregation(taskId, n);
            mapTasksAggregations.put(taskId, topNAggregation);
        }
        if (messageStatus.equals("first")) {
            long startTimeParsed = Long.parseLong(startTime);
            topNAggregation.setStartTime(startTimeParsed);
        }

        topNAggregation.incrementMessagesCount();
        for (int i = 0; i < tokensSize - 3; i++) {
            String token = tokens[i];
            String[] tokenTokens = token.split(":");

            if (tokenTokens.length == 2) {
                String key = tokenTokens[0];
                int freq = Integer.parseInt(tokenTokens[1]);

                if (messageStatus.equals("last")) {
                    topNAggregation.putDataToMerge(key, freq, messageId);
                } else {
                    topNAggregation.putDataToMerge(key, freq, -1);
                }
            }
        }
    }
}
