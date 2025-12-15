package com.itmo.ipkn.aggregator.aggregation;

import java.util.concurrent.atomic.AtomicInteger;

public class WordCounterAggregation {

    private int resultSum = 0;

    private final AtomicInteger messagesCount = new AtomicInteger(0);
    private int messagesAmountTotal = -1;

    private final String wordToCount;

    private long startTimeMillis = 0;
    private long endTimeMillis = 0;

    public WordCounterAggregation(String wordToCount, int initWordCount) {
        this.wordToCount = wordToCount;
        resultSum += initWordCount;
        messagesCount.addAndGet(1);
    }

    public void addToSum(int valueToAdd) {
        resultSum += valueToAdd;
        messagesCount.addAndGet(1);
        if (messagesAmountTotal != -1) {
            if (messagesCount.get() == messagesAmountTotal) {
                System.out.println("Result:");
                System.out.println("Word: " + wordToCount + " count: " + resultSum);
                System.out.println("For: " + (endTimeMillis - startTimeMillis) + "ms.");
            }
        }
    }

    public void setMessagesAmountTotal(int messagesAmountTotal) {
        this.messagesAmountTotal = messagesAmountTotal;
    }

    public void setStartTimeMillis(long startTimeMillis) {
        this.startTimeMillis = startTimeMillis;
    }

    public void setEndTimeMillis(long endTimeMillis) {
        this.endTimeMillis = endTimeMillis;
    }
}
