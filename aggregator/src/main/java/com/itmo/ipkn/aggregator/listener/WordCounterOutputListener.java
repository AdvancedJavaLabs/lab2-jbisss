package com.itmo.ipkn.aggregator.listener;

import com.itmo.ipkn.aggregator.aggregation.WordCounterAggregation;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WordCounterOutputListener {

    private final Map<Integer, WordCounterAggregation> map = new ConcurrentHashMap<>();

    @RabbitListener(queues = "wordCounterOutput")
    public void processMessage(String message) {
        String[] tokens = message.split("\\|");

        int taskId = Integer.parseInt(tokens[0]);
        int messageId = Integer.parseInt(tokens[1]);
        int wordCount = Integer.parseInt(tokens[2]);
        String wordToCount = tokens[3];
        String lastMessage = tokens[4];

        if (map.containsKey(taskId)) {
            WordCounterAggregation aggregation = map.get(taskId);
            if (lastMessage.equals("last")) {
                aggregation.setMessagesAmountTotal(messageId);
                aggregation.setEndTimeMillis(System.currentTimeMillis());
                aggregation.addToSum(wordCount);
            } else if (lastMessage.equals("first")) {
                aggregation.setStartTimeMillis(Long.parseLong(tokens[5]));
                aggregation.addToSum(wordCount);
            }else {
                aggregation.addToSum(wordCount);
            }
        } else {
            WordCounterAggregation newAggregation = new WordCounterAggregation(wordToCount, wordCount);
            if (lastMessage.equals("first")) {
                newAggregation.setStartTimeMillis(Long.parseLong(tokens[5]));
            }
            map.put(taskId, newAggregation);
        }
    }
}
