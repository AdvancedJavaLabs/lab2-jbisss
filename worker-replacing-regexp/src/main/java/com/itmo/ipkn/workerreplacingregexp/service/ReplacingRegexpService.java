package com.itmo.ipkn.workerreplacingregexp.service;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class ReplacingRegexpService {

    private static final List<String> RANDOM_WORDS
            = Arrays.asList("Лионель Месси", "Уилл Байерс", "Гендальф", "Криштиану Роналду", "Андрей Аршавин", "Артём Дзюба",
            "Арагорн", "Торин Дубощит", "Ларл Невиноват", "Гарри Поттер", "Рон Уизли", "Роберт Паттинсон");
    // Регулярное выражение:
    // Группа 1 (\\s|[.!?]) захватывает разделитель (пробел ИЛИ . ! ?)
    // Группа 2 ([А-ЯЁ][а-яё]+)\\b захватывает само слово
    // ^| перед всем выражением позволяет найти первое слово в строке
    private static final Pattern PATTERN = Pattern.compile("(^|[.!?])([А-ЯЁ][а-яё]+)\\b");
    private static final Random RANDOM = new Random();

    private final RabbitTemplate rabbitTemplate;

    public ReplacingRegexpService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void processMessageAndSendToAggregation(String message) {
        String[] tokens = message.split("\\|");

        int taskId = Integer.parseInt(tokens[0]);
        int messageId = Integer.parseInt(tokens[1]);
        String lineToHandle = tokens[2];
        String messageType = tokens[3];
        String timeIfHas = tokens[4];

        String handledMessage = replaceProperNounsSmartly(lineToHandle);
        StringBuilder outputMessage = new StringBuilder()
                .append(taskId)
                .append("|")
                .append(messageId)
                .append("|")
                .append(handledMessage)
                .append("|")
                .append(messageType)
                .append("|")
                .append(timeIfHas);

        System.out.println("Handling message " + messageId);
        rabbitTemplate.convertAndSend("replaceRegexQueueOutput", outputMessage.toString());
    }

    public String replaceProperNounsSmartly(String text) {
        return text.replaceAll("\\b[А-ЯЁA-Z][а-яёa-z]*\\b", getRandomWord());
    }

    private String getRandomWord() {
        return RANDOM_WORDS.get(RANDOM.nextInt(RANDOM_WORDS.size()));
    }
}
