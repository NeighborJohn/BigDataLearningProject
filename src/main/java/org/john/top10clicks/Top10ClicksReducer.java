package org.john.top10clicks;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class Top10ClicksReducer extends Reducer<Text, Text, Text, List<String>> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, List<String>>.Context context) throws IOException, InterruptedException {

        List<Long> tsList = new ArrayList<>();
        Map<Long, String> dataMap = new HashMap<>();

        for (Text value: values){
            String[] valueList = value.toString().split("_");
            String newsId = valueList[0];
            Long ts = Long.parseLong(valueList[1]);

            tsList.add(ts);

            dataMap.put(ts, newsId);
        }
        
        List<Long> tsSubList;
        
        if (tsList.size() >= 10){
//          对时间戳降序排序
            tsList.sort(Comparator.reverseOrder());
            tsSubList = tsList.subList(0,10);
        }
        else {
            tsSubList = tsList;
        }
        
        List<String> outPutList = new ArrayList<>();
//        String[] outPutList = new String[tsSubList.size()];

        for (long ts: tsSubList){
            outPutList.add(dataMap.get(ts) + "_" + ts);
        }

//        for (int i=0;i< tsSubList.size();i++){
//            outPutList[i] = dataMap.get(tsSubList.get(i));
//        }

        context.write(key, outPutList);
    }
}
