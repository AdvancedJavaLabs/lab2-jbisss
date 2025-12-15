package com.itmo.ipkn.aggregator.aggregation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TopNAggregation {

    private final Map<String, Integer> map = new ConcurrentHashMap<>();

    private final AtomicInteger messagesCount = new AtomicInteger(0);
    private int messagesCountTotal = -1;
    private long startTime;

    private final int taskId;
    private int n;

    public TopNAggregation(int taskId, int n) {
        this.taskId = taskId;
        this.n = n;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    private boolean isResultPrinted = false;

    public void putDataToMerge(String key, Integer freq, int lastMessageId) {
        map.merge(key, freq, Integer::sum);
        if (lastMessageId != -1) {
            messagesCountTotal = lastMessageId;
        }
        int andGet = messagesCount.get();
        if (andGet == messagesCountTotal && !isResultPrinted) {
            isResultPrinted = true;
            printResult();
        }
    }

    public void incrementMessagesCount() {
        messagesCount.incrementAndGet();
    }

    private void printResult() {
        System.out.println("Result for taskId: " + taskId);
        System.out.println("TopN: ");
        System.out.println(map.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(n)
                .toList()
        );
        System.out.println("For time: " + (System.currentTimeMillis() - startTime) + "ms.");
    }
}
