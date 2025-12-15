package com.itmo.ipkn.aggregator.listener;

import com.itmo.ipkn.aggregator.aggregation.SorterAggregation;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class SorterOutputListener {

    @RabbitListener(queues = "sorterQueueOutput")
    public void processMessage(String message) {
        String[] tokens = message.split("\\|");

        int taskId = Integer.parseInt(tokens[0]);
        int messageId = Integer.parseInt(tokens[1]);
        String sortedSentencesBody = tokens[2];
        String messageType = tokens[3];
        String timeIfHas = tokens[4];

        SorterAggregation sorterAggregation;
        if (SorterAggregation.aggregationMap.containsKey(taskId)) {
            sorterAggregation = SorterAggregation.aggregationMap.get(taskId);
        } else {
            sorterAggregation = new SorterAggregation(taskId);
            SorterAggregation.aggregationMap.put(taskId, sorterAggregation);
        }

        sorterAggregation.handleMessage(messageId, sortedSentencesBody, messageType, timeIfHas);
    }
}
