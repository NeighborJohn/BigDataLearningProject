package org.john.top10clicks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.john.Utils_hadoop;

import java.util.List;


public class Top10ClicksDriver {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("用户点击top10视频id");
        job.setJarByClass(Top10ClicksDriver.class);
        job.setMapperClass(Top10ClicksMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Top10ClicksReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(List.class);

        FileInputFormat.setInputPaths(job, new Path("/userclicks/train.csv"));

        if( Utils_hadoop.testExist(conf,"/userclicks/top10clicks_result")){
            Utils_hadoop.rmDir(conf,"/userclicks/top10clicks_result");}
        FileOutputFormat.setOutputPath(job, new Path("/userclicks/top10clicks_result"));
        job.waitForCompletion(true);
    }

}
