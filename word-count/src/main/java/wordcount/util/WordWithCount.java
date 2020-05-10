package wordcount.util;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor //无空参构造函数报错：Exception in thread "main" org.apache.flink.api.common.InvalidProgramException: This type (GenericType<wordcount.util.WordWithCount>) cannot be used as key.
public class WordWithCount {
    public String word;
    public long count;
}
