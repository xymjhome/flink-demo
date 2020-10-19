package wordcount.streamingjob.test;

import wordcount.util.WordWithCount;

public class ChildrenFlatMapDemo extends FlatMapDemo {

    @Override
    protected void addExtendValue(WordWithCount wordWithCount) {
        wordWithCount.demo = "demo";
    }
}
