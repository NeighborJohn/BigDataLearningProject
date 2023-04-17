package org.john.clicksnum;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class UserClicksReducer extends Reducer<Text,DoubleWritable,Text, ArrayList<Double>>{
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double clicksNum = 0.0;
        double sum = 0.0;

        ArrayList<Double> tmpArray = new ArrayList<>();

        for(DoubleWritable value: values) {
            clicksNum += value.get();
            sum++;
        }

        double rate = clicksNum/sum;

        tmpArray.add(clicksNum);
        tmpArray.add(rate);

//        DoubleWritable[] outPutArray = new DoubleWritable[tmpArray.size()];
//        for (int i=0;i<tmpArray.size();i++){
//            outPutArray[i] = new DoubleWritable(tmpArray.get(i));
//        }

//        context.write(key, new ArrayWritable(DoubleWritable.class, outPutArray));
        context.write(key, tmpArray);
    }
}
