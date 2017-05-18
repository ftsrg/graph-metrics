package metrics.degrees;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class AverageDegree extends BasicComputation<LongWritable, Text, Text, LongWritable>{

	@Override
	public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongWritable> messages) throws IOException {
		NumberFormat formatter = new DecimalFormat("#0.00");
		Text value = new Text(formatter.format((double) (2 * getTotalNumEdges()) / getTotalNumVertices()));
		vertex.setValue(value);
		vertex.voteToHalt();
	}

}
