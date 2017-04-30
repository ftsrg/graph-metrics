package metrics.degrees;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class OutDegree extends BasicComputation<LongWritable, Text, Text, LongWritable> {

	@Override
	public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongWritable> messages) throws IOException {
		if (getSuperstep() == 0) {
			sendMessageToAllEdges(vertex, new LongWritable(0));
		} else if (getSuperstep() == 1) {
			int messageCount = 0;
			for (@SuppressWarnings("unused")
			LongWritable message : messages) {
				messageCount += 1;
			}
			vertex.setValue(new Text(String.valueOf(messageCount)));
		}
		vertex.voteToHalt();
	}
}
