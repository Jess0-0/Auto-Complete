import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			// get and initialize n-gram data, set to 5 if value not provided
			Configuration configuration = context.getConfiguration();
			noGram = configuration.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// get sentence by sentence and remove useless characters
			String line = value.toString();
			line = line.trim().toLowerCase().replace("[^a-z]", " ");

			String[] words = line.split("\\s+");
			if (words.length < 2) {
				return;
			}

			StringBuilder phrase;

			// generate n-gram for each starting word
			for (int i = 0; i < words.length; ++i) {
				phrase = new StringBuilder();
				phrase.append(words[i]);
				// generate 2-gram to n-gram phrases and write to output
				for (int j = 1; i + j < words.length && j < noGram; ++j) {
					phrase.append(" ");
					phrase.append(words[i+j]);
					context.write(new Text(phrase.toString().trim()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//sum up the total count for each n-gram phrase\
			int sum = 0;
			for (IntWritable value: values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}