package com.itmo.ipkn.workerwordcounter.service;

import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class WordCounterService {

    public int countWordInText(String text, String word) {
        return Arrays.stream(text.split("[^\\p{L}\\p{Nd}]+")).toList().stream()
                .filter(wordInText -> wordInText.equals(word))
                .toList().size();
    }
}
