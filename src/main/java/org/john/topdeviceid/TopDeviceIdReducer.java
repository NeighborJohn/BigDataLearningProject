package org.john.topdeviceid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TopDeviceIdReducer extends Reducer<Text, Text, Text, List<Text>> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, List<Text>>.Context context) throws IOException, InterruptedException {

        Map<Text, Long> deviceMap = new HashMap<>();
        for (Text deviceId:values){
            if (deviceMap.containsKey(deviceId)) {
                deviceMap.replace(deviceId, deviceMap.get(deviceId)+1);
            }
            else {
                deviceMap.put(deviceId, (long) 1);
            }
        }
        List<Map.Entry<Text, Long>> list = new ArrayList<>(deviceMap.entrySet()); //转换为list
//      按Map的value值对list进行排序，返回-result可以进行降序排序
        list.sort((o1, o2) -> {
            int result = o1.getValue().compareTo(o2.getValue());
            return -result;
        });

        List<Text> outPutList = new ArrayList<>();
        for (int i=0;i<list.size()&&i<3;i++){
            String str= list.get(i).getKey().toString() + "_" + list.get(i).getValue();
            outPutList.add(new Text(str));
        }

        context.write(key, outPutList);

    }
}
