package com.itmo.ipkn.producer.util;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitQueueConfiguration {

    public static final String WORD_COUNTER_QUEUE = "wordCounterQueue";
    public static final String TOP_N_QUEUE = "topNQueue";
    public static final String SENTIMENT_ANALYZER_QUEUE = "sentimentAnalyzerQueue";
    public static final String REPLACING_REGEXP_QUEUE = "replaceRegexQueue";
    public static final String SORTER_QUEUE = "sorterQueue";

    public static final String WORD_COUNTER_QUEUE_OUTPUT = "wordCounterOutput";
    public static final String TOP_N_QUEUE_OUTPUT = "topNQueueOutput";
    public static final String SENTIMENT_ANALYZER_QUEUE_OUTPUT = "sentimentAnalyzerQueueOutput";
    public static final String REPLACING_REGEXP_QUEUE_OUTPUT = "replaceRegexQueueOutput";
    public static final String SORTER_QUEUE_OUTPUT = "sorterQueueOutput";

    @Bean
    public Queue wordCounterQueue() {
        return new Queue(WORD_COUNTER_QUEUE, true);
    }

    @Bean
    public Queue wordCounterQueueOutput() {
        return new Queue(WORD_COUNTER_QUEUE_OUTPUT, true);
    }

    @Bean
    public Queue topNQueue() {
        return new Queue(TOP_N_QUEUE, true);
    }

    @Bean
    public Queue topNQueueOutput() {
        return new Queue(TOP_N_QUEUE_OUTPUT, true);
    }

    @Bean
    public Queue sentimentAnalyzerQueue() {
        return new Queue(SENTIMENT_ANALYZER_QUEUE, true);
    }

    @Bean
    public Queue sentimentAnalyzerQueueOutput() {
        return new Queue(SENTIMENT_ANALYZER_QUEUE_OUTPUT, true);
    }

    @Bean
    public Queue replaceRegexQueue() {
        return new Queue(REPLACING_REGEXP_QUEUE, true);
    }

    @Bean
    public Queue replaceRegexQueueOutput() {
        return new Queue(REPLACING_REGEXP_QUEUE_OUTPUT, true);
    }


    @Bean
    public Queue sorterQueue() {
        return new Queue(SORTER_QUEUE, true);
    }

    @Bean
    public Queue sorterQueueOutPut() {
        return new Queue(SORTER_QUEUE_OUTPUT, true);
    }
}
