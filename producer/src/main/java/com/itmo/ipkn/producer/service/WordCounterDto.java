package com.itmo.ipkn.producer.service;

public class WordCounterDto {

    private int taskId;
    private int messageId;
    private String wordToCount;
    private String textLine;
    private String lastMessage;

    public WordCounterDto(int taskId, int messageId, String wordToCount, String textLine, String lastMessage) {
        this.taskId = taskId;
        this.messageId = messageId;
        this.wordToCount = wordToCount;
        this.textLine = textLine;
        this.lastMessage = lastMessage;
    }

    public WordCounterDto() {}

    public int getTaskId() {
        return taskId;
    }

    public int getMessageId() {
        return messageId;
    }

    public String getWordToCount() {
        return wordToCount;
    }

    public String getTextLine() {
        return textLine;
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public void setWordToCount(String wordToCount) {
        this.wordToCount = wordToCount;
    }

    public void setTextLine(String textLine) {
        this.textLine = textLine;
    }

    public void setLastMessage(String lastMessage) {
        this.lastMessage = lastMessage;
    }
}
