package metrics.nedc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import util.MessageWrapper;

public class NEDCMessageWrapper extends MessageWrapper<LongWritable, Text, Text> {

	@Override
	public Class<LongWritable> getVertexIdClass() {
		return LongWritable.class;
	}

	@Override
	public Class<Text> getMessageClass() {
		return Text.class;
	}

	@Override
	public Class<Text> getEdgeValueClass() {
		return Text.class;
	}

}
