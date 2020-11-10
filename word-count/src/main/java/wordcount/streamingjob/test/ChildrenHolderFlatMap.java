package wordcount.streamingjob.test;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import wordcount.util.WordWithCount;

/**
 * Created by liujiangtao1 on 18:40 2020-08-14.
 *
 * @Description:
 */
public class ChildrenHolderFlatMap extends RichFlatMapFunction<String, WordWithCount> {

    private ChildrenFlatMapDemo delegte;

    public ChildrenHolderFlatMap(ChildrenFlatMapDemo delegte) {
        this.delegte = delegte;
    }

    @Override
    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
        delegte.flatMap(value, out);
    }
}
