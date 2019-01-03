package cn.zy.order.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: zhangying
 * @Date: 2019/1/3 14:50
 * @Version 1.0
 */
public class OrderBean implements WritableComparable<OrderBean>{
    private String orderId;
    private Double price;

    public OrderBean() {
    }

    public OrderBean(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public int compareTo(OrderBean o) {
            // 1 先按订单id排序(从小到大)
        int result = this.orderId.compareTo(o.getOrderId());

        if (result == 0) {
            // 2 再按金额排序（从大到小）
            result = price > o.getPrice() ? -1 : 1;
        }

        return result;
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);//string
        dataOutput.writeDouble(price);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();//接受string
        this.price = dataInput.readDouble();


    }
}
