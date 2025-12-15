package com.itmo.ipkn.workertopn.service.rabbit.listener;

import com.itmo.ipkn.workertopn.service.TopNService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class WordTopNListener {

    private final TopNService topNService;

    public WordTopNListener(TopNService topNService) {
        this.topNService = topNService;
    }

    @RabbitListener(queues = "topNQueue")
    public void processMessage(String message) {
        topNService.getTopNFromTextAndSendToAggregator(message);
    }
}
