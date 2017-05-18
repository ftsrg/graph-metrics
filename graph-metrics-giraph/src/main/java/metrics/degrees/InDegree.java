package metrics.degrees;
import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class InDegree extends BasicComputation<LongWritable, Text, Text, LongWritable> {

	@Override
	public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongWritable> messages) throws IOException {
		if (getSuperstep() == 0) {
			sendMessageToAllEdges(vertex, new LongWritable(0));
		} else if (getSuperstep() == 1) {
			long inDegree = 0;
			for (@SuppressWarnings("unused") LongWritable lw : messages) {
				inDegree++;
			}
			vertex.setValue(new Text(String.valueOf(inDegree)));
			vertex.voteToHalt();
		}
	}
}
