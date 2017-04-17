import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class OutDegree extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	
	private static final Logger LOG = Logger.getLogger(OutDegree.class);

	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages)
			throws IOException {
		
		if (getSuperstep() == 0) {
			sendMessageToAllEdges(vertex, new DoubleWritable(0.0));
		} else if (getSuperstep() == 1) {
			int msg = 0;
			for (@SuppressWarnings("unused") DoubleWritable message : messages) {
				msg += 1;
			}
			vertex.setValue(new DoubleWritable(msg));
		}
		vertex.voteToHalt();
		
	}

}
