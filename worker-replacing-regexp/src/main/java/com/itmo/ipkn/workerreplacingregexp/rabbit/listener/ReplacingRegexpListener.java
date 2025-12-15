package com.itmo.ipkn.workerreplacingregexp.rabbit.listener;

import com.itmo.ipkn.workerreplacingregexp.service.ReplacingRegexpService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class ReplacingRegexpListener {

    private final ReplacingRegexpService replacingRegexpService;

    public ReplacingRegexpListener(ReplacingRegexpService replacingRegexpService) {
        this.replacingRegexpService = replacingRegexpService;
    }

    @RabbitListener(queues = "replaceRegexQueue")
    public void processMessage(String message) {
        replacingRegexpService.processMessageAndSendToAggregation(message);
    }
}
