package wordcount.streamingjob.test;

import org.apache.flink.configuration.Configuration;
import wordcount.util.WordWithCount;

public class ChildrenFlatMapDemo extends FlatMapDemo {

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("ChildrenFlatMapDemo run !!!");
        super.open(parameters);
    }

    @Override
    protected void addExtendValue(WordWithCount wordWithCount) {
        wordWithCount.demo = "demo";
    }
}
