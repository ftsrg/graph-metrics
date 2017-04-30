package metrics.clustering;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import util.LongArrayListWritable;
import util.MessageWrapper;

public class LongIdFriendsList extends MessageWrapper<LongWritable, LongArrayListWritable, Text> {

	@Override
	public Class<LongWritable> getVertexIdClass() {
		return LongWritable.class;
	}

	@Override
	public Class<LongArrayListWritable> getMessageClass() {
		return LongArrayListWritable.class;
	}

	@Override
	public Class<Text> getEdgeValueClass() {
		return Text.class;
	}

}
