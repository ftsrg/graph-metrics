package metrics.clustering;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import util.LongArrayListWritable;

public class LocalClusteringCoefficient {
	
	public static class VertexInDegreeComputation extends BasicComputation<LongWritable, Text, Text, LCCMessageWrapper> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LCCMessageWrapper> messages)
				throws IOException {
			LCCMessageWrapper neighbours = new LCCMessageWrapper();
			neighbours.setSourceId(vertex.getId());
			neighbours.setMessage(new LongArrayListWritable());
			neighbours.setEdgeValue(new Text());
			sendMessageToAllEdges(vertex, neighbours);
		}
		
	}
	
	public static class VertexInOutDegreeComputation extends BasicComputation<LongWritable, Text, Text, LCCMessageWrapper> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LCCMessageWrapper> messages)
				throws IOException {
			long inDegree = 0;
			for (@SuppressWarnings("unused") LCCMessageWrapper message : messages) {
				inDegree++;
			}
			long outDegree = vertex.getNumEdges();
			long inOutDegree = inDegree + outDegree;
			vertex.setValue(new Text(String.valueOf(inOutDegree)));
			
			LongArrayListWritable friends = new LongArrayListWritable();
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				friends.add(edge.getTargetVertexId());
			}
			for (LCCMessageWrapper msg : messages) {
				friends.add(msg.getSourceId());
			}
			LCCMessageWrapper message = new LCCMessageWrapper();
			message.setSourceId(vertex.getId());
			message.setMessage(friends);
			message.setEdgeValue(new Text());
			sendMessageToAllEdges(vertex, message);
			for (LCCMessageWrapper msg : messages) {
				sendMessage(msg.getSourceId(), message);
			}
		}
		
	}

	public static class NeighboursLinkComputation
			extends BasicComputation<LongWritable, Text, Text, LCCMessageWrapper> {
		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LCCMessageWrapper> messages)
				throws IOException {

			for (LCCMessageWrapper message : messages) {
				long commonNeighbours = 0;
				for (LongWritable id : message.getMessage()) {
					if (vertex.getEdgeValue(id) != null) {
						commonNeighbours++;
					}
				}
				LongArrayListWritable commonNeighboursCount = new LongArrayListWritable();
				commonNeighboursCount.add(new LongWritable(commonNeighbours));
				LCCMessageWrapper newMessage = new LCCMessageWrapper();
				newMessage.setSourceId(vertex.getId());
				newMessage.setMessage(commonNeighboursCount);
				newMessage.setEdgeValue(new Text());
				sendMessage(message.getSourceId(), newMessage);
			}

		}
	}
	
	public static class LCCComputation extends BasicComputation<LongWritable, Text, Text, LCCMessageWrapper> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LCCMessageWrapper> messages)
				throws IOException {

			long lcc = 0;
			for (LCCMessageWrapper message : messages) {
				lcc += message.getMessage().get(0).get();
			}
			long inOutDegree = Long.parseLong(vertex.getValue().toString());
			System.out.println("INOUTDEGREE: " + vertex.getId() + ": " + inOutDegree);
			NumberFormat formatter = new DecimalFormat("#0.00");
			String format;
			if (inOutDegree != 1) {
				format = formatter.format((double) (lcc) / (inOutDegree * (inOutDegree - 1)));
			} else {
				format = formatter.format((double) (lcc) / (inOutDegree));
			}
			vertex.setValue(new Text(String.valueOf(format)));
			vertex.voteToHalt();
		}

	}

	public static class MasterCompute extends DefaultMasterCompute {

		@Override
		public final void compute() {
			long superstep = getSuperstep();
			if (superstep == 0) {
				setComputation(VertexInDegreeComputation.class);
			} else if (superstep == 1) {
				setComputation(VertexInOutDegreeComputation.class);
			} else if (superstep == 2) {
				setComputation(NeighboursLinkComputation.class);
			} else if (superstep == 3) {
				setComputation(LCCComputation.class);
			}
		}
	}
}
