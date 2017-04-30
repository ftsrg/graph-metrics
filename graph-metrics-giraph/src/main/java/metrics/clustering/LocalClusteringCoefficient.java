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
	
	public static class VertexInDegreeComputation extends BasicComputation<LongWritable, Text, Text, LongIdFriendsList> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongIdFriendsList> messages)
				throws IOException {
			vertex.setValue(new Text(String.valueOf(vertex.getNumEdges())));
			LongIdFriendsList neighbours = new LongIdFriendsList();
			neighbours.setSourceId(vertex.getId());
			neighbours.setMessage(new LongArrayListWritable());
			neighbours.setEdgeValue(new Text());
			sendMessageToAllEdges(vertex, neighbours);
		}
		
	}
	
	public static class VertexInOutDegreeComputation extends BasicComputation<LongWritable, Text, Text, LongIdFriendsList> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongIdFriendsList> messages)
				throws IOException {
			long inDegree = 0;
			for (@SuppressWarnings("unused") LongIdFriendsList message : messages) {
				inDegree++;
			}
			long outDegree = Long.parseLong(vertex.getValue().toString());
			long inOutDegree = inDegree + outDegree;
			vertex.setValue(new Text(String.valueOf(inOutDegree)));
		}
		
	}

	public static class SendFriendsList extends BasicComputation<LongWritable, Text, Text, LongIdFriendsList> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongIdFriendsList> messages)
				throws IOException {

			final LongArrayListWritable friends = new LongArrayListWritable();

			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				friends.add(WritableUtils.clone(edge.getTargetVertexId(), getConf()));
			}
			LongIdFriendsList message = new LongIdFriendsList();
			message.setSourceId(vertex.getId());
			message.setMessage(friends);
			message.setEdgeValue(new Text());
			sendMessageToAllEdges(vertex, message);
		}

	}

	public static class NeighboursLinkComputation
			extends BasicComputation<LongWritable, Text, Text, LongIdFriendsList> {
		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongIdFriendsList> messages)
				throws IOException {

			for (LongIdFriendsList message : messages) {
				long commonNeighbours = 0;
				for (LongWritable id : message.getMessage()) {
					if (vertex.getEdgeValue(id) != null) {
						commonNeighbours++;
					}
				}
				LongArrayListWritable commonNeighboursCount = new LongArrayListWritable();
				commonNeighboursCount.add(new LongWritable(commonNeighbours));
				LongIdFriendsList newMessage = new LongIdFriendsList();
				newMessage.setSourceId(vertex.getId());
				newMessage.setMessage(commonNeighboursCount);
				newMessage.setEdgeValue(new Text());
				sendMessage(message.getSourceId(), newMessage);
			}

		}
	}
	
	public static class LCCComputation extends BasicComputation<LongWritable, Text, Text, LongIdFriendsList> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<LongIdFriendsList> messages)
				throws IOException {

			long lcc = 0;
			for (LongIdFriendsList message : messages) {
				lcc += message.getMessage().get(0).get();
			}
			long inOutDegree = Long.parseLong(vertex.getValue().toString());
			System.out.println("INOUTDEGREE: " + vertex.getId() + ": " + inOutDegree);
			NumberFormat formatter = new DecimalFormat("#0.00");
			String format = formatter.format((double) (2 * lcc) / (inOutDegree * (inOutDegree - 1)));
			vertex.setValue(new Text(format));
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
				setComputation(SendFriendsList.class);
			} else if (superstep == 3) {
				setComputation(NeighboursLinkComputation.class);
			} else if (superstep == 4) {
				setComputation(LCCComputation.class);
			}
		}
	}
}
