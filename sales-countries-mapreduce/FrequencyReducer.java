import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text country, Iterable<IntWritable> count, Context context) throws IOException, InterruptedException {
        int country_count = 0;
        for (IntWritable counts: count) {
            country_count += counts.get();
        }
        context.write(country, new IntWritable(country_count));
    }
}
