package com.itmo.ipkn.aggregator.aggregation;

import java.util.concurrent.atomic.AtomicInteger;

public class SentimentAnalyzerAggregator {

    private final int taskId;

    private int lastMessageId = -1;
    private final AtomicInteger messageCounter = new AtomicInteger(0);
    private long startTime = 0;

    private double sumCoef = 0;

    public SentimentAnalyzerAggregator(int taskId) {
        this.taskId = taskId;
    }

    public void handleMessage(int messageId, double coef, String messageType, String timeIfHas) {
        int andGet = messageCounter.incrementAndGet();
        sumCoef += coef;
        if (messageType.equals("last")) {
            lastMessageId = messageId;
        } else if (messageType.equals("first")) {
            startTime = Long.parseLong(timeIfHas);
        }
        if (andGet == lastMessageId) {
            printResult();
        }
    }

    private void printResult() {
        System.out.println("For task " + taskId);
        System.out.println("Result coefficient: " + sumCoef / messageCounter.get());
        System.out.println("For time " + (System.currentTimeMillis() - startTime) + "ms.");
    }
}
