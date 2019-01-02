package cn.zy.flowCount.mapper;

import java.io.IOException;

import cn.zy.flowCount.writable.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
* <LongWritable, Text, Text, FlowBean>，
* 前两个是 要读取数据的key-value （数据 的行号，对应的数据），
* 后两个是处理数据后的输出的key-value （手机号，FlowBean对象）
* */
public class FlowMapper  extends Mapper<LongWritable, Text, Text, FlowBean> {

    FlowBean bean = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        // 1 获取一行数据，将一行内容转成string
        String ling = value.toString();

        // 2 截取数据，切分字段
        String[] fields = ling.split("\t");

        // 3 取出数据
        // 手机号码
        String phoneNum = fields[1];

        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        //4 把获取到的流量数据，封装成flowbean对象

        bean.setDownFlow(downFlow);
        bean.setUpFlow(upFlow);
        k.set(phoneNum);//电话号

        // 5 写出数据
        /*
         * 写出的数据是k-v,即电话号-流量信息
         * */
        context.write(k,bean);//直接new对象，会浪费内存;
    }

}
