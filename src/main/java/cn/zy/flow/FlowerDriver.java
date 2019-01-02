package cn.zy.flow;
import cn.zy.flow.partitioner.FlowPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/*
* win10本地模式运行，需要额外2个jar包
* hadoop-mapreduce-client-common
* hadoop-mapreduce-client-jobclient
*
* https://blog.csdn.net/qq_20120669/article/details/53374418
* */
public class FlowerDriver {
    public  static  void  main(String [] args){
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowerDriver.class);

        // 3 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        // 4 指定mapper输出数据的kv类型
        job.setMapOutputValueClass(FlowBean.class);

        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6.设置分区
        job.setPartitionerClass(FlowPartitioner.class);
        //设置分区数量
        job.setNumReduceTasks(5);

        // 6 指定job的输入原始文件所在目录
        try {
            FileInputFormat.setInputPaths(job, new Path(args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = false;
        try {
            result = job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.exit(result ? 0 : 1);
    }
}
