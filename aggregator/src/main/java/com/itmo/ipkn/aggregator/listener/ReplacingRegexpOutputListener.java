package com.itmo.ipkn.aggregator.listener;

import com.itmo.ipkn.aggregator.service.ReplacingRegexpService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class ReplacingRegexpOutputListener {

    private final ReplacingRegexpService replacingRegexpService;

    public ReplacingRegexpOutputListener(ReplacingRegexpService replacingRegexpService) {
        this.replacingRegexpService = replacingRegexpService;
    }

    @RabbitListener(queues = "replaceRegexQueueOutput")
    public void processMessage(String message) {
        replacingRegexpService.processMessage(message);
    }
}
