import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
  @Override
  public void reduce(Text character, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
    long sum  = 0;
    for (LongWritable count : counts) {
      sum += count.get();
    }
    context.write(character, new LongWritable(sum));
  }
}
