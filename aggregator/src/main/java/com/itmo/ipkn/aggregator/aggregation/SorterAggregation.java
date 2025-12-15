package com.itmo.ipkn.aggregator.aggregation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SorterAggregation {

    public static final Map<Integer, SorterAggregation> aggregationMap = new ConcurrentHashMap<>();

    private final PriorityBlockingQueue<String> queue = new PriorityBlockingQueue<>();

    private int lastMessageId = -1;
    private final AtomicInteger messageCounter = new AtomicInteger(0);
    private long startTime = 0;

    private final int taskId;

    public SorterAggregation(int taskId) {
        this.taskId = taskId;
    }

    public void handleMessage(int messageId, String sentencesBody, String messageType, String timeIfHas) {
        String[] sentences = sentencesBody.split("\n");
        for (String line : sentences) {
            if (line != null && !line.trim().isEmpty()) {
                queue.offer(line.trim());
            }
        }
        int andGet = messageCounter.incrementAndGet();
        if (messageType.equals("last")) {
            lastMessageId = messageId;
        } else if (messageType.equals("first")) {
            startTime = Long.parseLong(timeIfHas);
        }

        if(andGet == lastMessageId){
            printResultToFile();
        }
    }

    private void printResultToFile() {
        System.out.println("Task proceeded for " + (System.currentTimeMillis() - startTime) + "ms.");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("./output/sorted_" + this.taskId + ".txt"))) {
            String element;
            while ((element = queue.poll()) != null) {
                writer.write(element);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        aggregationMap.remove(this.taskId);
    }
}
