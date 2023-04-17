package org.john.clicksnum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.john.Utils_hadoop;

import java.util.ArrayList;

public class UserClicksDriver {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("用户点击率");
        job.setJarByClass(UserClicksDriver.class);
        job.setMapperClass(UserClicksMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(UserClicksReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayList.class);

        FileInputFormat.setInputPaths(job, new Path("/userclicks/train.csv"));

        if( Utils_hadoop.testExist(conf,"/userclicks/userclicks_result")){
            Utils_hadoop.rmDir(conf,"/userclicks/userclicks_result");}
        FileOutputFormat.setOutputPath(job, new Path("/userclicks/userclicks_result"));
        job.waitForCompletion(true);
    }

}
