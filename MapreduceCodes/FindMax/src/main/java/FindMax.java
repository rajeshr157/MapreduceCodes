import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindMax {
    public static class FindMaxMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text max = new Text("Max");
        private IntWritable num = new IntWritable();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(), ",");

            while (itr.hasMoreTokens()) {
                num.set(Integer.parseInt(itr.nextToken()));
                context.write(max, num);
            }
        }
    }


    public static class FindMaxReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int maxnum = 0;

            for (IntWritable val : values) {
                if (val.get() > maxnum)
                    maxnum = val.get();
            }

            result.set(maxnum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(FindMax.class);
        job.setMapperClass(FindMaxMapper.class);
        job.setCombinerClass(FindMaxReducer.class);
        job.setReducerClass(FindMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}