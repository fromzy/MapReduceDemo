package cn.zy.wordcount;
        import java.io.IOException;

        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Mapper;


public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    /**
     * map阶段的业务逻辑就写在自定义的map()方法中
     * maptask会对每一行输入数据调用一次自定义的map（）方法
     */

    @Override //map函数接受文本，把结果以key-value的形式存储，
    // key-->LongWritable即文本的行号，value-->Text即被切割的每一行文本；
    protected void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        super.map(key, value, context);

        //1.获取一行数据

        String line =value.toString();

        //2.获取行中每一个单词,以 空格 切割
        String words[] = line.split(" ");
        for (String word : words){
            System.out.print(word);

            //3.输出单词
            //输出到maptask自己内部的缓冲区，在缓冲区中整理数据（分区，排序，合并...）,最后到reduceTask里；
            // 将单词作为key，将次数1作为value,以便于后续的数据分发，可以根据单词分发，以便于相同单词会到相同的reducetask中
            context.write(new Text(word),new IntWritable(1));//key-->Text是每个单词,value-->IntWritable是单词个数，
        }
    }
}
