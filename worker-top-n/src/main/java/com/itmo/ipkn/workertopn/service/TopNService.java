package com.itmo.ipkn.workertopn.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class TopNService {

    private final RabbitTemplate rabbitTemplate;

    public TopNService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void getTopNFromText(String text, int n) {
        String[] words = text
                .toLowerCase()
                .replaceAll("[^a-zа-я0-9ё]+", " ")
                .trim()
                .split("\\s+");

        Map<String, Long> freq = Arrays.stream(words)
                .collect(Collectors.groupingBy(w -> w, Collectors.counting()));

        List<String> topN = freq.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .map(Map.Entry::getKey)
                .limit(n)
                .collect(Collectors.toList());

        rabbitTemplate.convertAndSend("topNOutput", topN);
    }
}
