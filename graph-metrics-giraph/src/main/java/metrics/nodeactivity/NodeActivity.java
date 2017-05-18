package metrics.nodeactivity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import util.DimensionType;

public class NodeActivity {

	public static class SendEdgeValues extends BasicComputation<LongWritable, Text, Text, Text> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<Text> messages) throws IOException {
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				sendMessage(edge.getTargetVertexId(), edge.getValue());
			}

		}

	}

	public static class NodeActivityComputation extends BasicComputation<LongWritable, Text, Text, Text> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<Text> messages) throws IOException {
			Set<DimensionType> dimensions = new HashSet<>();
			for (Text message : messages) {
				dimensions.add(DimensionType.getEnumByName(message.toString()));
			}
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				dimensions.add(DimensionType.getEnumByName(edge.getValue().toString()));
			}
			vertex.setValue(new Text(String.valueOf(dimensions.size())));
			vertex.voteToHalt();
		}
	}

	public static class MasterCompute extends DefaultMasterCompute {

		@Override
		public final void compute() {
			long superstep = getSuperstep();
			if (superstep == 0) {
				setComputation(SendEdgeValues.class);
			} else if (superstep == 1) {
				setComputation(NodeActivityComputation.class);
			}
		}
	}
}
