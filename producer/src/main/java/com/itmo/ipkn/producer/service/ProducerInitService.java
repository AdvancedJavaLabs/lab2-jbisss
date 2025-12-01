package com.itmo.ipkn.producer.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static com.itmo.ipkn.producer.util.RabbitQueueConfiguration.WORD_COUNTER_QUEUE;
import static com.itmo.ipkn.producer.util.RabbitQueueConfiguration.TOP_N_QUEUE;
import static com.itmo.ipkn.producer.util.RabbitQueueConfiguration.SENTIMENT_ANALYZER_QUEUE;
import static com.itmo.ipkn.producer.util.RabbitQueueConfiguration.REPLACING_REGEXP_QUEUE;
import static com.itmo.ipkn.producer.util.RabbitQueueConfiguration.SORTER_QUEUE;

@Service
public class ProducerInitService {

    private static final String MEDIUM = "medium";
    private static final String LARGE = "large";

    private static final String SMALL_FILE_NAME = "smallText.txt";
    private static final String MEDIUM_FILE_NAME = "mediumText.txt";
    private static final String LARGE_FILE_NAME = "largeText.txt";

    private final RabbitTemplate rabbitTemplate;

    public ProducerInitService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void initWordCounter(String fileSize, String word) {
        readFileAndSentToRabbitForWordFinder(defineFileNameBySize(fileSize), WORD_COUNTER_QUEUE, word);
    }

    public void initTopN(String fileSize) {
        readFileAndSentToRabbit(defineFileNameBySize(fileSize), TOP_N_QUEUE);
    }

    public void initSentimentAnalyzer(String fileSize) {
        readFileAndSentToRabbit(defineFileNameBySize(fileSize), SENTIMENT_ANALYZER_QUEUE);
    }

    public void initReplacingRegexp(String fileSize) {
        readFileAndSentToRabbit(defineFileNameBySize(fileSize), REPLACING_REGEXP_QUEUE);
    }

    public void initSorter(String fileSize) {
        readFileAndSentToRabbit(defineFileNameBySize(fileSize), SORTER_QUEUE);
    }

    private String defineFileNameBySize(String fileSize) {
        return switch (fileSize) {
            case MEDIUM -> MEDIUM_FILE_NAME;
            case LARGE -> LARGE_FILE_NAME;
            default -> SMALL_FILE_NAME;
        };
    }

    private void readFileAndSentToRabbitForWordFinder(String fileName, String queueName, String word) {
        try (BufferedReader reader = new BufferedReader(new FileReader("producer/src/main/resources/static/" + fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().equals("")) {
                    rabbitTemplate.convertAndSend(queueName, word + "|" + line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readFileAndSentToRabbit(String fileName, String queueName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                rabbitTemplate.convertAndSend(queueName, line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
