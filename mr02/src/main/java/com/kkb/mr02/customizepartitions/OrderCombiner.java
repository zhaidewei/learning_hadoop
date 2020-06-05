package com.kkb.mr02.customizepartitions;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderCombiner extends Reducer<Text, OrderBean, Text, OrderBean> {

    @Override
    protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
        OrderBean max = new OrderBean();
        Float maxPrice = 0f;

        for (OrderBean ob: values) {
            Float price = ob.getPrice();
            if (price >= maxPrice) {
                max.setProductid(ob.getProductid());
                max.setPrice(ob.getPrice());
                maxPrice = price;
            }
        }
        context.write(key, max);
    }
}
