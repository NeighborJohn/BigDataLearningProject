package org.john.clicksnum;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserClicksMapper extends Mapper<LongWritable,Text,Text, DoubleWritable>{

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {

        Text text = new Text();
        DoubleWritable doubleWritable = new DoubleWritable();

//        跳过首行表头
        if (key.toString().equals("0")) {
            System.out.println("这是首行表头，跳过...");
        }
        else {
            // 按“,“分隔输入内容
            String values = value.toString();
            String[] value_list = values.split(",");

            /*
            no comment
            String guid = value_list[5];
            String newsid = value_list[4];
            long target = Long.parseLong(value_list[1]);
            */

            text.set(value_list[5]);
            doubleWritable.set(Double.parseDouble(value_list[1]));

            context.write(text, doubleWritable);
        }
    }
}
