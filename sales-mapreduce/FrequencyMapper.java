import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  private final static LongWritable one = new LongWritable(1);
  @Override
  public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
    String line = lineText.toString();
    for (int i = 0; i < line.length(); i++) {
      Text character = new Text(line.substring(i, i+1));
      context.write(new Text(character), one);
    }
  }
}
