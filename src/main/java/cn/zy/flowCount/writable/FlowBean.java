package cn.zy.flowCount.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

// bean对象要实例化
public class FlowBean implements Writable {

    private long upFlow;    //上行流量
    private long downFlow;  //下行流量
    private long sumFlow;   //总流量

    // 反序列化时，需要通过反射调用无参构造器，所以必须有无参构造器
    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }


// 序列化
    /*
    * 序列化，即把内存中的对象写到磁盘中
    * */

    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

//反序列化
    /*
    * 注意反序列化的顺序和序列化的顺序完全一致，
    * 反序列化，把磁盘中的对象读入内存中
    * */


    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    //要想把结果显示在文件中，需要重写toString()，且用”\t”分开，方便后续用
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}

