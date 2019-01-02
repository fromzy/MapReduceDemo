package cn.zy.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer 类，也有四个泛型，同理，分别指的是reduce 函数输入的key、value类型
// （这里输入的key、value类型通常和map的输出key、value类型保持一致）和输出的key、value 类型。
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * key，是一组相同单词kv对的key
     */

    @Override//key，是一组相同单词kv对的key
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        super.reduce(key, values, context);

        int count = 0;

        // 1 汇总各个key的个数
        for(IntWritable value:values){
            count +=value.get();
        }

        // 2输出该key的总次数
        context.write(key, new IntWritable(count));
    }
}
