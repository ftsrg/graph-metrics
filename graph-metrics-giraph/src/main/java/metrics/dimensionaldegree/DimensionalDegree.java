package metrics.dimensionaldegree;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import util.DimensionType;

public class DimensionalDegree {
	
	public static class InOutEdgesComputation extends BasicComputation<LongWritable, Text, Text, MessageWithSenderAndEdgeType> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<MessageWithSenderAndEdgeType> messages)
				throws IOException {
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				MessageWithSenderAndEdgeType message = new MessageWithSenderAndEdgeType();
				message.setSourceId(vertex.getId());
				message.setMessage(vertex.getValue());
				sendMessage(edge.getTargetVertexId(), message);
			}
		}
	}
	
	public static class DimensionalDegreeComputation extends BasicComputation<LongWritable, Text, Text, MessageWithSenderAndEdgeType> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<MessageWithSenderAndEdgeType> messages) throws IOException {
			long dimensionalDegree = 0;
			final DimensionType dimension = DimensionType.OUTGOING;
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				if (edge.getValue().toString().equals(dimension.getLabel())) {
					dimensionalDegree++;
				}
			}
			for (MessageWithSenderAndEdgeType message : messages) {
				String messageLabel = message.getMessage().toString();
				if (messageLabel.equals(dimension.getLabel())) {
					dimensionalDegree++;
				}
			}
			vertex.setValue(new Text(String.valueOf(dimensionalDegree)));
			vertex.voteToHalt();
		}
	}
	
	public static class MasterCompute extends DefaultMasterCompute {
		
		@Override
		public final void compute() {
			long superstep = getSuperstep();
			if (superstep == 0) {
				setComputation(InOutEdgesComputation.class);
			} else if (superstep == 1) {
				setComputation(DimensionalDegreeComputation.class);
			}
		}
	}
}
