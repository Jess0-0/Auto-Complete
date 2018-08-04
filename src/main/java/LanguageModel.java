import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		// get the threshold to filter less frequent words
		@Override
		public void setup(Context context) {
			Configuration configuration = new Configuration();
			threashold = configuration.getInt("threashold", 20); // default to 20
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}

			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");

			// check for bad input
			if(wordsPlusCount.length != 2) {
				return;
			}

			int count = Integer.valueOf(wordsPlusCount[1]);

			// check and ignore infrequent inputs
			if (count < threashold) {
				return;
			}

			String[] words = wordsPlusCount[0].split("\\s+");

			StringBuilder outputKey = new StringBuilder();

			for (int i = 0; i < words.length - 1; ++i) {
				outputKey.append(words[i] + " ");
			}

			String outKey = outputKey.toString().trim();
			String outValue = words[words.length - 1] + "=" + count;

			context.write(new Text(outKey), new Text(outValue));

		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int topK;

		// get the topK parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("n", 5); // default value set to top5
		}

		//use TreeMap to rank topN n-gram, then write out to HDFS
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// initialize treemap
			TreeMap<Integer, List<String>> map = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
			for (Text value: values) {
				String cur = value.toString().trim();
				String word = cur.split("=")[0];
				int freq = Integer.parseInt(cur.split("=")[1]);
				if (map.containsKey(freq)) {
					map.get(freq).add(word);
				} else {
					List<String> entry = new ArrayList<String>();
					entry.add(word);
					map.put(freq, entry);
				}
			}

			// iterate over treemap to find the topk entries
			Iterator<Integer> iter = map.keySet().iterator();
			for (int j = 0; j < topK && iter.hasNext();) {
				int freq = iter.next();
				List<String> words = map.get(freq);
				for (String word: words) {
					context.write(new DBOutputWritable(key.toString(), word, freq), NullWritable.get());
					j++;
				}
			}

		}
	}
}
