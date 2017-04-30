package metrics.dimensionalclustering;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import util.MessageWrapper;

public class MapFriendsList extends MessageWrapper<LongWritable, MapWritable, Text> {

	@Override
	public Class<LongWritable> getVertexIdClass() {
		return LongWritable.class;
	}

	@Override
	public Class<MapWritable> getMessageClass() {
		return MapWritable.class;
	}

	@Override
	public Class<Text> getEdgeValueClass() {
		return Text.class;
	}

}

