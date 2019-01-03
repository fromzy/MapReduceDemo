package cn.zy.order.partitioner;

import cn.zy.order.writable.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: zhangying
 * @Date: 2019/1/3 15:02
 * @Version 1.0
 */
/*
* 泛型对应着map的输出k-v
* */
public class OrderPatitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean key, NullWritable value, int numReduceTasks) {

        return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
