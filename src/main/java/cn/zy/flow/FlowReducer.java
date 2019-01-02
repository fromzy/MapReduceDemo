package cn.zy.flow;

import cn.zy.flow.writable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/*
* <Text, FlowBean, Text, FlowBean>，
* reducer的输入就是map的输出，即手机号-FlowBean对象
* 输出也是机号-FlowBean对象
* */
public class FlowReducer  extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean resultBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;

        // 1 遍历所用bean，将其中的总的上行流量，总的下行流量分别
        for (FlowBean bean : values) {
            sum_upFlow += bean.getUpFlow();
            sum_downFlow += bean.getDownFlow();
        }

        resultBean.setDownFlow(sum_downFlow);
        resultBean.setUpFlow(sum_upFlow);
        // 2 封装对象
        context.write(key, resultBean);
    }
}

