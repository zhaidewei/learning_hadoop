package com.kkb.mr02;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {

    private Integer upFlow;
    private Integer downFlow;
    private Integer upCountFlow;
    private Integer downCountFlow;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(upCountFlow);
        dataOutput.writeInt(downCountFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.upCountFlow = dataInput.readInt();
        this.downCountFlow = dataInput.readInt();
    }

    public Integer getUpFlow() {return upFlow;}
    public Integer getDownFlow() {return downFlow;}
    public Integer getUpCountFlow() {return upCountFlow;}
    public Integer getDownCountFlow() {return downCountFlow;}

    public void setUpFlow(Integer x) {upFlow=x;}
    public void setDownFlow(Integer x) {downFlow=x;}
    public void setUpCountFlow(Integer x) {upCountFlow=x;}
    public void setDownCountFlow(Integer x) {downCountFlow=x;}

    @Override
    public String toString() {
        return "flowBean={"
                + "upflow=" + upFlow.toString() + ","
                + "downflow=" + downFlow.toString() + ","
                + "upcountflow=" + upCountFlow.toString() + ','
                + "downcountflow=" + downCountFlow.toString()
                + "}";
    }
}
