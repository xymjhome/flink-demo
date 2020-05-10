package wordcount.util;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WordWithFrequency {
    public String word;

    //此字段代替count，因为sql语句中，count是一个统计函数，写sql语句时会报错
    // Table sqlQuery = tableEnvironment
    //            .sqlQuery("select word as word, sum(count) as count from WordCount group by word");
    public long frequency;
}
