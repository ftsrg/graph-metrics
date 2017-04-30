package metrics.dimensionalclustering;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DimensionalClustering2 {

	public static class SendOutEdges extends BasicComputation<LongWritable, Text, Text, MapFriendsList> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<MapFriendsList> messages)
				throws IOException {
			MapFriendsList message = new MapFriendsList();
			message.setSourceId(vertex.getId());
			message.setMessage(new MapWritable());
			message.setEdgeValue(new Text());
			sendMessageToAllEdges(vertex, message);
		}
	}

	public static class SendFriendsListToAllEdges
			extends BasicComputation<LongWritable, Text, Text, MapFriendsList> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<MapFriendsList> messages)
				throws IOException {
			MapWritable friends = new MapWritable();
			for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
				friends.put(WritableUtils.clone(edge.getTargetVertexId(), getConf()), new Text());
			}
			for (MapFriendsList msg : messages) {
				friends.put(msg.getSourceId(), new Text());
			}
			MapFriendsList message = new MapFriendsList();
			message.setSourceId(vertex.getId());
			message.setMessage(friends);
			message.setEdgeValue(new Text());
			sendMessageToAllEdges(vertex, message);
			for (MapFriendsList msg : messages) {
				sendMessage(msg.getSourceId(), message);
			}
		}
	}

	public static class NeighboursLinkComputation
			extends BasicComputation<LongWritable, Text, Text, MapFriendsList> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<MapFriendsList> messages)
				throws IOException {
			for (MapFriendsList msg : messages) {
				MapFriendsList message = new MapFriendsList();
				MapWritable links = new MapWritable();
				for (Writable id : msg.getMessage().keySet()) {
					if (vertex.getEdgeValue((LongWritable) id) != null) {
						links.put(id, vertex.getEdgeValue((LongWritable) id));
					}
				}
				message.setSourceId(vertex.getId());
				Text edgeValue = vertex.getEdgeValue(msg.getSourceId());
				message.setEdgeValue(edgeValue != null ? edgeValue : new Text());
				message.setMessage(links);
				sendMessage(msg.getSourceId(), message);
			}
		}
	}

	public static class DimensionalClustering2Computation
			extends BasicComputation<LongWritable, Text, Text, MapFriendsList> {

		@Override
		public void compute(Vertex<LongWritable, Text, Text> vertex, Iterable<MapFriendsList> messages)
				throws IOException {
			List<MapFriendsList> messageList = new ArrayList<>();
			for (MapFriendsList msg : messages) {
				MapFriendsList modifiedList = new MapFriendsList();
				modifiedList.setMessage(msg.getMessage());
				modifiedList.setSourceId(msg.getSourceId());
				if (msg.getEdgeValue().toString().equals("")) {
					modifiedList.setEdgeValue(vertex.getEdgeValue(msg.getSourceId()));
				} else {
					modifiedList.setEdgeValue(msg.getEdgeValue());
				}
				messageList.add(modifiedList);
			}
			long twoTriads = 0;
			long threeTriangles = 0;
			for (int i = 0; i < messageList.size(); i++) {
				MapFriendsList firstMessage = messageList.get(i);
				Text firstEdgeValue = firstMessage.getEdgeValue();
				for (int j = i + 1; j < messageList.size(); j++) {
					MapFriendsList secondMessage = messageList.get(j);
					Text secondEdgeValue = secondMessage.getEdgeValue();
					// Ha különbözik a két él típusa
					if (!firstEdgeValue.equals(secondEdgeValue)) {
						// van-e a kettő között él valamelyik irányban?
						boolean hasEdge = false;
						for (Writable id : firstMessage.getMessage().keySet()) {
							Text acrossEdgeValue = (Text) firstMessage.getMessage().get(id);
							if (((LongWritable) id).get() == secondMessage.getSourceId().get()
									&& !acrossEdgeValue.equals(firstEdgeValue) && 
									!acrossEdgeValue.equals(secondEdgeValue)) {
								hasEdge = true;
								break;
							}
						}
						if (!hasEdge) {
							for (Writable id : secondMessage.getMessage().keySet()) {
								Text acrossEdgeValue = (Text) secondMessage.getMessage().get(id);
								if (((LongWritable) id).get() == firstMessage.getSourceId().get()
										&& !acrossEdgeValue.equals(firstEdgeValue) &&
										!acrossEdgeValue.equals(secondEdgeValue)) {
									hasEdge = true;
									break;
								}
							}
						}
						if (hasEdge) {
							threeTriangles++;
						} else {
							twoTriads++;
						}
					}
				}
			}
			NumberFormat formatter = new DecimalFormat("#0.00");
			double dc1 = 0.0;
			if (twoTriads != 0) {
				dc1 = (double) threeTriangles / twoTriads;
			}
			String format = formatter.format(dc1);
			vertex.setValue(new Text(format));
			vertex.voteToHalt();
		}
	}
	
	public static class MasterCompute extends DefaultMasterCompute {

		@Override
		public final void compute() {
			long superstep = getSuperstep();
			if (superstep == 0) {
				setComputation(SendOutEdges.class);
			} else if (superstep == 1) {
				setComputation(SendFriendsListToAllEdges.class);
			} else if (superstep == 2) {
				setComputation(NeighboursLinkComputation.class);
			} else if (superstep == 3) {
				setComputation(DimensionalClustering2Computation.class);
			}
		}
	}
	
}
