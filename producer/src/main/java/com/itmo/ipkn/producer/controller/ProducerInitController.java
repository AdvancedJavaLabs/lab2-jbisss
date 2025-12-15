package com.itmo.ipkn.producer.controller;

import com.itmo.ipkn.producer.service.ProducerInitService;
import jakarta.websocket.server.PathParam;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerInitController {

    private final ProducerInitService producerInitService;

    public ProducerInitController(ProducerInitService producerInitService) {
        this.producerInitService = producerInitService;
    }

    @GetMapping(value = "/init/wordCounter/{word}")
    public void initWordCounter(@PathParam("fileSize") String fileSize, @PathVariable String word) {
        producerInitService.initWordCounter(fileSize, word);
    }

    @GetMapping("/init/TopN/{n}")
    public void initTopN(@PathParam("fileSize") String fileSize, @PathVariable int n) {
        producerInitService.initTopN(fileSize, n);
    }

    @GetMapping("/init/SentimentAnalyzer")
    public void initSentimentAnalyzer(@PathParam("fileSize") String fileSize) {
        producerInitService.initSentimentAnalyzer(fileSize);
    }

    @GetMapping("/init/ReplacingRegexp")
    public void initReplacingRegexp(@PathParam("fileSize") String fileSize) {
        producerInitService.initReplacingRegexp(fileSize);
    }

    @GetMapping("/init/Sorter")
    public void initSorter(@PathParam("fileSize") String fileSize) {
        producerInitService.initSorter(fileSize);
    }
}
