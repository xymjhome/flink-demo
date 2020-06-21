package com.analysis.project.uniquevisitor;


public class SimpleBloomHash {

    private int cap;

    public SimpleBloomHash(int cap) {
        this.cap = cap;
    }


    public Long hash(String element, int seed) {
        long result = 0;
        for (int i = 0; i < element.length(); i++) {
            result = result * seed + element.charAt(i);
        }

        return result & (cap - 1);
    }
}
