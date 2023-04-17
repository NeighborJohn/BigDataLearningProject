package org.john.TopDeviceId;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopDeviceIdMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        if (key.get()==0){
            System.out.println("这是首行表头，跳过...");
        }
        else{
            String[] valueList = value.toString().split(",");
            String deviceId = valueList[3];
            String guid = valueList[5];
            context.write(new Text(guid), new Text(deviceId));
        }
    }
}
