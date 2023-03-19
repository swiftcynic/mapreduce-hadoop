import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    @Override
    public void reduce(Text title, Iterable<DoubleWritable> ratings, Context context) throws IOException, InterruptedException {
        double net_ratings = 0;
        double count = 0;
        for (DoubleWritable rating : ratings) {
            net_ratings += rating.get();
            count += 1.0;
        }
        net_ratings /= count;
        context.write(title,  new DoubleWritable(net_ratings));
    }
}
