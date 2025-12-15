package com.itmo.ipkn.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final AtomicInteger taskIdSeq = new AtomicInteger(1);

    public ProducerInitService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void initWordCounter(String fileSize, String word) {
        readFileAndSentToRabbitForWordFinder(defineFileNameBySize(fileSize), WORD_COUNTER_QUEUE, word);
    }

    public void initTopN(String fileSize, int n) {
        readFileAndSentToRabbitForTopN(defineFileNameBySize(fileSize), TOP_N_QUEUE, n);
    }

    public void initSentimentAnalyzer(String fileSize) {
        readFileAndSentToRabbit(defineFileNameBySize(fileSize), SENTIMENT_ANALYZER_QUEUE);
    }

    public void initReplacingRegexp(String fileSize) {
        readFileAndSentToRabbit(defineFileNameBySize(fileSize), REPLACING_REGEXP_QUEUE);
    }

    public void initSorter(String fileSize) {
        readFileAndSentToRabbitForSorter(defineFileNameBySize(fileSize), SORTER_QUEUE);
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
            String line = reader.readLine();
            String nextLine;

            AtomicInteger counter = new AtomicInteger(1);

            int taskId = taskIdSeq.getAndIncrement();
            int firstMessage = 1;

            while (line != null) {
                if (!line.trim().isEmpty()) {
                    reader.mark(10000);
                    nextLine = reader.readLine();
                    reader.reset();

                    boolean isLastLine = (nextLine == null);

                    WordCounterDto wordCounterDto = new WordCounterDto(taskId, counter.getAndIncrement(), word, line, "false");

                    if (firstMessage == 1) {
                        wordCounterDto.setLastMessage("first");
                    }
                    if (isLastLine) {
                        wordCounterDto.setLastMessage("last");
                    }
                    String dtoAsString = objectMapper.writeValueAsString(wordCounterDto);
                    rabbitTemplate.convertAndSend(queueName, dtoAsString);
                    firstMessage = 2;
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private final int batchLineSize = 25;

    private void readFileAndSentToRabbitForTopN(String fileName, String queueName, int n) {
        try (BufferedReader reader = new BufferedReader(new FileReader("producer/src/main/resources/static/" + fileName))) {
            String line = reader.readLine();
            String nextLine;

            AtomicInteger counter = new AtomicInteger(1);

            int taskId = taskIdSeq.getAndIncrement();
            int firstMessage = 1;

            while (line != null) {
                List<String> linesToSend = new ArrayList<>();

                boolean isLastLine = false;

                for (int i = 0; i < batchLineSize;) {
                    if (!line.trim().isEmpty()) {
                        i++;
                        reader.mark(10000);
                        nextLine = reader.readLine();
                        reader.reset();

                        isLastLine = (nextLine == null);

                        linesToSend.add(line);

                        if (isLastLine) {
                            break;
                        }
                    }
                    line = reader.readLine();
                }

                String messageToSend = taskId + "|" + counter.getAndIncrement() + "|" + n + "|" + String.join("\n", linesToSend) + "|";

                if (firstMessage == 1) {
                    messageToSend += "first|" + System.currentTimeMillis();
                } else if (isLastLine) {
                    messageToSend += "last|d";
                } else {
                    messageToSend += "false|a";
                }

                rabbitTemplate.convertAndSend(queueName, messageToSend);
                firstMessage = 2;
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private final int BATCH_SIZE = 25;
    private final String SEPARATOR = "|";
    private final String LINE_SEPARATOR = "\n";

    private void readFileAndSentToRabbitForSorter(String fileName, String queueName) {
        List<String> sentenceBatch = new ArrayList<>();
        boolean isFirstBatch = true;

        BreakIterator sentenceIterator = BreakIterator.getSentenceInstance(new Locale("ru", "RU"));

        int taskId = taskIdSeq.getAndIncrement();
        AtomicInteger counter = new AtomicInteger(1);

        try (BufferedReader reader = new BufferedReader(new FileReader("producer/src/main/resources/static/" + fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sentenceIterator.setText(line);
                int start = sentenceIterator.first();
                for (int end = sentenceIterator.next(); end != BreakIterator.DONE; start = end, end = sentenceIterator.next()) {
                    String sentence = line.substring(start, end).trim();
                    if (!sentence.isEmpty()) {
                        sentenceBatch.add(sentence);

                        if (sentenceBatch.size() == BATCH_SIZE) {
                            sendBatch(taskId, counter.getAndIncrement(), sentenceBatch, queueName, isFirstBatch, false);
                            sentenceBatch.clear();
                            isFirstBatch = false;
                        }
                    }
                }
            }

            if (!sentenceBatch.isEmpty()) {
                sendBatch(taskId, counter.getAndIncrement(), sentenceBatch, queueName, isFirstBatch, true);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendBatch(int taskId, int messageId, List<String> sentences, String queueName, boolean first, boolean last) {
        String messageBody = sentences.stream().collect(Collectors.joining(LINE_SEPARATOR));

        String marker;
        if (first) {
            marker = "first|" + System.currentTimeMillis();
        } else if (last) {
            marker = "last|d";
        } else {
            marker = "false|a";
        }

        String messageToSend = taskId + SEPARATOR + messageId + SEPARATOR + messageBody + SEPARATOR + marker;

        rabbitTemplate.convertAndSend(queueName, messageToSend);
        System.out.println("Отправлен батч с меткой: " + marker + ", размер: " + sentences.size());
    }

    private void readFileAndSentToRabbit(String fileName, String queueName) {
        try (BufferedReader reader = new BufferedReader(new FileReader("producer/src/main/resources/static/" + fileName))) {
            String line = reader.readLine();
            String nextLine;

            AtomicInteger counter = new AtomicInteger(1);

            int taskId = taskIdSeq.getAndIncrement();
            int firstMessage = 1;

            while (line != null) {
                if (!line.trim().isEmpty()) {
                    reader.mark(10000);
                    nextLine = reader.readLine();
                    reader.reset();

                    boolean isLastLine = (nextLine == null);

                    String messageToSend = taskId + "|" + counter.getAndIncrement() + "|" + line + "|";

                    if (firstMessage == 1) {
                        messageToSend += "first|" + System.currentTimeMillis();
                    } else if (isLastLine) {
                        messageToSend += "last|d";
                    } else {
                        messageToSend += "false|a";
                    }
                    rabbitTemplate.convertAndSend(queueName, messageToSend);
                    firstMessage = 2;
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
