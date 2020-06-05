package com.kkb.mr02.customizepartitions;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<Text, OrderBean, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
        Text maxProductId = new Text();
        Float maxPrice = 0f;

        for (OrderBean ob: values) {
            Float price = ob.getPrice();
            String productiId = ob.getProductid();

            if (price >= maxPrice) {
                maxProductId.set(productiId);
                maxPrice = price;
            }
        }
        context.write(key, maxProductId);
    }
}
