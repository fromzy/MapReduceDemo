package cn.zy.order.mapper;

/**
 * @Author: zhangying
 * @Date: 2019/1/3 15:46
 * @Version 1.0
 */
import java.io.IOException;

import cn.zy.order.writable.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
*
* 输出只有key没有value,故value是NullWritable
* */
public class OrderSortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
    OrderBean bean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)throws IOException, InterruptedException {
        // 1 获取一行数据
        String line = value.toString();

        // 2 截取字段
        String[] fields = line.split("\t");

        // 3 封装bean
        bean.setOrderId(fields[0]);
        bean.setPrice(Double.parseDouble(fields[2]));
        


        // 4 写出
        context.write(bean, NullWritable.get());
    }
}
