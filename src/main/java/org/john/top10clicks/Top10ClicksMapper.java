package org.john.top10clicks;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Top10ClicksMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        if (key.get() == 0) {
            System.out.println("这是首行表头，跳过...");
        }
        else {
            String[] values = value.toString().split(",");

            String guid = values[5];
            String newsId = values[4];
            String ts = values[values.length-1];

//          mapper输出为 key:guid  value: newsID_ts
            context.write(new Text(guid), new Text(newsId + "_" + ts));
        }
    }
}
