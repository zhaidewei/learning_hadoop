package com.kkb.mr02.customizepartitions;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String productid;
    private Float price;

    @Override
    public String toString() {
        return "Order={productid=" + productid + ", " + "price=" + price + "}";
    }

    @Override
    public int compareTo(OrderBean o) {
        return price.compareTo(o.price);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(productid);
        dataOutput.writeFloat(price);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.productid = dataInput.readUTF();
        this.price = dataInput.readFloat();
    }

    public String getProductid() {return this.productid;}
    public Float getPrice() {return this.price;}
    public void setProductid(String s) {this.productid=s;}
    public void setPrice(Float f) {this.price=f;}
}
