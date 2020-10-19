package wordcount.streamingjob.test;

import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import wordcount.util.WordWithCount;

public abstract class FlatMapDemo extends RichFlatMapFunction<String, WordWithCount> {

    @Override
    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
        String[] words = value.split("\\s");
        Arrays.stream(words)
            .filter(StringUtils::isNotBlank)
            .forEach(word -> {
                WordWithCount wordWithCount = new WordWithCount();
                wordWithCount.word = word;
                wordWithCount.count = 1;
                addExtendValue(wordWithCount);
                out.collect(wordWithCount);
            });
    }

    protected abstract void addExtendValue(WordWithCount wordWithCount);


}
