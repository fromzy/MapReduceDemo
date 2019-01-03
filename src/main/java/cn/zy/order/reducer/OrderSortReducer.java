package cn.zy.order.reducer;

/**
 * @Author: zhangying
 * @Date: 2019/1/3 15:47
 * @Version 1.0
 */
import java.io.IOException;

import cn.zy.order.writable.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
/*
* 泛型前两个对应着map的输出，
* */
public class OrderSortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
    @Override
    protected void reduce(OrderBean bean, Iterable<NullWritable> values,
                          Context context) throws IOException, InterruptedException {
        // 直接写出
        context.write(bean, NullWritable.get());
    }
}

