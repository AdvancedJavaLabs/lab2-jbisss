package com.itmo.ipkn.workersorter.service.rabbit.listener;

import com.itmo.ipkn.workersorter.service.SorterService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class SorterListener {

    private final SorterService sorterService;

    public SorterListener(SorterService sorterService) {
        this.sorterService = sorterService;
    }

    @RabbitListener(queues = "sorterQueue")
    public void processMessage(String message) {
        sorterService.sortSentencesAndSendToAggregator(message);
    }
}
