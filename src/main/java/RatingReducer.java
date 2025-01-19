import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class RatingReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private final IntWritable result = new IntWritable();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        try {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
