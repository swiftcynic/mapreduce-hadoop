import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper
extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  @Override
  public void map(LongWritable offset, Text lineText, Context context)
  throws IOException, InterruptedException {
    String line = lineText.toString();
    String[] values = line.split(",");
    String title = values[0];
    double rating = 0.0;
    try {
       rating = Double.parseDouble(values[1].strip());
    }
    catch(NumberFormatException exception) { }
    if (rating >= 8.0) {
       context.write(new Text(title), new DoubleWritable(rating));
    }
  }
}
