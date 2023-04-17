package org.john.topdeviceid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.john.Utils_hadoop;

import java.io.IOException;
import java.util.List;

public class TopDeviceIdDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("用户使用最多的设备");
        job.setJarByClass(TopDeviceIdDriver.class);
        job.setMapperClass(TopDeviceIdMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TopDeviceIdReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(List.class);

        FileInputFormat.setInputPaths(job, new Path("/userclicks/train.csv"));

        if( Utils_hadoop.testExist(conf,"/userclicks/topdeviceid_result")){
            Utils_hadoop.rmDir(conf,"/userclicks/topdeviceid_result");}
        FileOutputFormat.setOutputPath(job, new Path("/userclicks/topdeviceid_result"));
        job.waitForCompletion(true);
    }
}
