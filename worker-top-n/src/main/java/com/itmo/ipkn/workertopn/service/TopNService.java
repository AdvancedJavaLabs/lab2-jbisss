package com.itmo.ipkn.workertopn.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class TopNService {

    private final RabbitTemplate rabbitTemplate;

    public TopNService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void getTopNFromTextAndSendToAggregator(String message) {
        String[] tokens = message.split("\\|");

        int taskId = Integer.parseInt(tokens[0]);
        int messageId = Integer.parseInt(tokens[1]);
        int n = Integer.parseInt(tokens[2]);
        String textLine = tokens[3];
        String messageStatus = tokens[4];

        long startTime = System.currentTimeMillis();

        String[] words = textLine
                .toLowerCase()
                .replaceAll("[^a-zа-я0-9ё]+", " ")
                .trim()
                .split("\\s+");

        Map<String, Long> freq = Arrays.stream(words)
                .filter(word -> word.length() > 2)
                .collect(Collectors.groupingBy(w -> w, Collectors.counting()));

        List<Map.Entry<String, Long>> topN = freq.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(n)
                .toList();

        StringBuilder messageOutput = new StringBuilder();
        int size = topN.size();
        AtomicInteger i = new AtomicInteger(0);
        for (Map.Entry<String, Long> entry : topN) {
            messageOutput.append(entry.getKey()).append(":").append(entry.getValue());
            int andGet = i.incrementAndGet();
            if (size != andGet) {
                messageOutput.append("|");
            }
        }

        messageOutput.append("|").append(n).append("|").append(startTime).append("|").append(messageId).append("|").append(taskId).append("|").append(messageStatus);

        System.out.println("Handling message: " + messageId);
        rabbitTemplate.convertAndSend("topNQueueOutput", messageOutput.toString());
    }
}
