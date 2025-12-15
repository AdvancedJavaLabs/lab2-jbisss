package com.itmo.ipkn.aggregator.service;

import com.itmo.ipkn.aggregator.aggregation.ReplacingRegexpAggregation;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ReplacingRegexpService {

    private final Map<Integer, ReplacingRegexpAggregation> aggregationMap = new ConcurrentHashMap<>();

    public void processMessage(String message) {
        ReplacingRegexpAggregation aggregation = null;
        try {
            String[] tokens = message.split("\\|");

            int taskId = Integer.parseInt(tokens[0]);
            int messageId = Integer.parseInt(tokens[1]);
            String textLine = tokens[2];
            String messageType = tokens[3];
            String timeIfHas = tokens[4];

            if (aggregationMap.containsKey(taskId)) {
                aggregation = aggregationMap.get(taskId);
            } else {
                aggregation = new ReplacingRegexpAggregation(taskId, "rabbit_messages_" + taskId);
                aggregationMap.put(taskId, aggregation);
            }

            aggregation.handleMessage(messageId, messageType, timeIfHas);
            String messageToWrite = messageId + ":" + textLine;

            aggregation.getWriteSemaphore().acquire();
            aggregation.getTempFileWriter().write(messageToWrite);
            aggregation.getTempFileWriter().newLine();
            aggregation.getTempFileWriter().flush();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        } finally {
            if (aggregation != null) {
                aggregation.getWriteSemaphore().release();
            }
        }
    }
}
