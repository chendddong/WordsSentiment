import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SentimentAnalysis {

    public static class SentimentSplit extends Mapper<Object, Text, Text, IntWritable> {
        /* Simple cache for comparison later */
        public Map<String, String> emotionDic = new HashMap<String, String>();

        @Override
        public void setup(Context context) throws IOException{
            Configuration configuration = context.getConfiguration();
            /* Get from the set */
            String dicName = configuration.get("dictionary", "");
            /* Load the dictionary */
            BufferedReader br = new BufferedReader(new FileReader(dicName));
            /* Read per line due to the txt data structure */
            String line = br.readLine();

            while (line != null) {
                /* Break the words by tab */
                String[] word_feeling = line.split("\t");
                /* Key-Value : word - word_sentiment */
                emotionDic.put(word_feeling[0].toLowerCase(), word_feeling[1]);
                line = br.readLine();
            }
            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context) throws
                IOException, InterruptedException {

            /* Get the file name */
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString().trim();
            String[] words = line.split("\\s+"); /* By one or more spaces */
            for (String word: words) {
                /* Only write those in the Map */
                /* Primary key must add the file name */
                if (emotionDic.containsKey(word.trim().toLowerCase())) {
                    /* Just need the total number of Positive, neutral and negative words */
                    context.write(new Text(fileName + "\t" + emotionDic.get(word.toLowerCase())), new IntWritable(1));
                }
            }

        }
    }

    public static class SentimentCollection extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        /* Set the dic and then load the dic to the memo */
        configuration.set("dictionary", args[2]);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentSplit.class);
        job.setReducerClass(SentimentCollection.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
