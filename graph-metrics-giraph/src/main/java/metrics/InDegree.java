package metrics;
import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class InDegree extends BasicComputation<LongWritable, Text, Text, LongWritable> {

	@Override
	public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongWritable> messages) throws IOException {
		vertex.setValue(new Text(String.valueOf(vertex.getNumEdges())));
		vertex.voteToHalt();	
	}
}
