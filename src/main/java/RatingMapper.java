import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class RatingMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final IntWritable rating = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // Split the line by tabs
        String[] tokens = value.toString().split("\t");
        // Extract the rating (third column)
        if (tokens.length == 4) {
            rating.set(Integer.parseInt(tokens[2]));
            context.write(rating, one);
        }

    }
}
