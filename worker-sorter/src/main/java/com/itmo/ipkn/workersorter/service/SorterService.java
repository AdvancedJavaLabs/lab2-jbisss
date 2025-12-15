package com.itmo.ipkn.workersorter.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class SorterService {

    private final RabbitTemplate rabbitTemplate;

    private static final String SEPARATOR = "|";
    private static final String INNER_BATCH_SEPARATOR = "\n";

    public SorterService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sortSentencesAndSendToAggregator(String message) {
        String[] tokens = message.split("\\|");

        int taskId = Integer.parseInt(tokens[0]);
        int messageId = Integer.parseInt(tokens[1]);
        String sentencesBody = tokens[2];
        String messageType = tokens[3];
        String timeIfHas = tokens[4];

        List<String> sentences = Arrays.asList(sentencesBody.split(INNER_BATCH_SEPARATOR));

        List<String> sortedSentences = sentences.stream()
                .sorted(Comparator.comparingInt(String::length))
                .collect(Collectors.toList());

        String sortedMessageBody = String.join(INNER_BATCH_SEPARATOR, sortedSentences);

        String messageToSend = taskId + SEPARATOR + messageId + SEPARATOR + sortedMessageBody + SEPARATOR + messageType + SEPARATOR + timeIfHas;

        rabbitTemplate.convertAndSend("sorterQueueOutput", messageToSend);
        System.out.println("Handled message: " + messageId);
    }
}
