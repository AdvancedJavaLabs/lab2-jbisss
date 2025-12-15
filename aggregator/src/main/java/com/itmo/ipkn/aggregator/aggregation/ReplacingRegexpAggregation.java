package com.itmo.ipkn.aggregator.aggregation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ReplacingRegexpAggregation {

    private final int taskId;

    private Path tempFilePath;
    private BufferedWriter tempFileWriter;
    private final Semaphore writeSemaphore = new Semaphore(1);

    private final AtomicInteger messageCount = new AtomicInteger(0);
    private int lastMessageId = -1;
    private long startTime;

    public ReplacingRegexpAggregation(int taskId, String tempFilePath) {
        this.taskId = taskId;
        try {
            this.tempFilePath = Files.createTempFile("./output/" + tempFilePath, ".tmp");
            this.tempFileWriter = Files.newBufferedWriter(this.tempFilePath, StandardOpenOption.APPEND);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void handleMessage(int messageId, String messageType, String timeIfHas) {
        int andGet = messageCount.incrementAndGet();
        if (messageType.equals("last")) {
            lastMessageId = messageId;
        } else if (messageType.equals("first")) {
            startTime = Long.parseLong(timeIfHas);
        }
        if (andGet == lastMessageId) {
            try {
                String finalFileName = "./output/final_result_for_task_" + taskId;
                finalizeFile(Files.createTempFile(finalFileName, ".tmp"));
                System.out.println("Task " + taskId + " has been processed.");
                System.out.println("Watch file: " + finalFileName);
                System.out.println("For: " + (System.currentTimeMillis() - startTime) + "ms.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void finalizeFile(Path finalOutputPath) throws IOException {
        tempFileWriter.close();

        List<String> lines = Files.readAllLines(tempFilePath);

        lines.sort((s1, s2) -> {
            Long num1 = Long.parseLong(s1.split("\\|", 2)[0]);
            Long num2 = Long.parseLong(s2.split("\\|", 2)[0]);
            return num1.compareTo(num2);
        });

        List<String> sortedMessagesOnly = lines.stream()
                .map(line -> line.split(":", 2)[1])
                .collect(Collectors.toList());

        Files.write(finalOutputPath, sortedMessagesOnly, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.delete(tempFilePath);
    }

    public BufferedWriter getTempFileWriter() {
        return tempFileWriter;
    }

    public Semaphore getWriteSemaphore() {
        return writeSemaphore;
    }
}
