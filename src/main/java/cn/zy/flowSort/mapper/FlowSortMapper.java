package cn.zy.flowSort.mapper;

import cn.zy.flowSort.writable.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: zhangying
 * @Date: 2019/1/2 14:07
 * @Version 1.0
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean bean = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 1 拿到的是上一个统计程序输出的结果，已经是各手机号的总流量信息
        String line = value.toString();

        // 2 截取字符串并获取电话号、上行流量、下行流量
        String[] fields = line.split("\t");
        String phoneNbr = fields[0];

        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        // 3 封装对象
        bean.setDownFlow(downFlow);
        bean.setUpFlow(upFlow);
        v.set(phoneNbr);

        // 4 输出
        context.write(bean, v);
    }
}

