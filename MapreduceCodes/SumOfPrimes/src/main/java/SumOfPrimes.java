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

public class SumOfPrimes {
    public static class SumOfPrimeMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text("Sum of Prime");
        private IntWritable num = new IntWritable();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(), ",");

            while (itr.hasMoreTokens()) {

                int numint = Integer.parseInt(itr.nextToken());

                int flag = 1;


                num.set(numint);
                if (numint == 1)
                    context.write(word, num);
                else if (numint == 2)
                    context.write(word, num);
                else if (flag == 1) {

                    for (int i = 2; i < numint; i++) {
                        if (numint % i == 0)
                            flag = 0;
                    }
                    if (flag == 1)
                        context.write(word, num);
                }
            }
        }
    }


    public static class SumOfPrimeReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);


        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Odd Even Sum");
        job.setJarByClass(SumOfPrimes.class);
        job.setMapperClass(SumOfPrimeMapper.class);
        job.setCombinerClass(SumOfPrimeReducer.class);
        job.setReducerClass(SumOfPrimeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}