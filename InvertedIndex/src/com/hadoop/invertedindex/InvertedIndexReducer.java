package com.hadoop.invertedindex;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		int count = 0;
		
		for (Text val : values) {
			String str = val.toString();			
			if(map != null && map.get(str) != null){
				count = (int)map.get(str);
				map.put(str, ++count);
			}
			else{
				map.put(str, 1);
			}

		}
		
		context.write(key, new Text(map.toString()));
	}

}
