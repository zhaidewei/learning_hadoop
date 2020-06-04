package com.kkb.mr02.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private String phone;
    private Integer upFlow;
    private Integer downFlow;
    private Integer upCount;
    private Integer downCount;

    @Override
    public String toString() {
        return  phone + "\t" + upFlow + "\t" +downFlow + "\t" + upCount + "\t" + downCount ;
    }

    @Override
    public int compareTo(FlowBean o) {
        int i = this.downFlow.compareTo(o.downFlow);
        if (i == 0) {
            i = o.upCount.compareTo(this.upCount); //希望反着用就是降序了
        }
        return i;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(upCount);
        dataOutput.writeInt(downCount);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF();
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.upCount = dataInput.readInt();
        this.downCount = dataInput.readInt();
    }

    public String getPhone() {return phone;}
    public int getUpFlow() {return upFlow;}
    public int getDownFlow() {return downFlow;}
    public int getUpCount() {return upCount;}
    public int getDownCount() {return downCount;}

    public void setPhone(String p) {phone=p;}
    public void setUpFlow(int i) {upFlow=i;}
    public void setDownFlow(int i) {downFlow=i;}
    public void setUpCount(int i) {upCount=i;}
    public void setDownCount(int i) {downCount=i;}
}
