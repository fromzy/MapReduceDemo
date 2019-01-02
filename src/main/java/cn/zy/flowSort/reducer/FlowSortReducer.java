package cn.zy.flowSort.reducer;

import cn.zy.flowSort.writableComparable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: zhangying
 * @Date: 2019/1/2 14:16
 * @Version 1.0
 */
public class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean bean, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        context.write(values.iterator().next(), bean);
    }
}

