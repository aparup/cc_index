package org.ccindex.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;
import org.archive.hadoop.ArchiveJSONViewLoader;
import org.archive.hadoop.ResourceRecordReader;



public class WATFileRecordReader extends RecordReader<Text, Text>{

	ArchiveJSONViewLoader watFileLoader;
	
	Tuple currentTuple;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		String[] tupleArray = new String[2];
		tupleArray[0] = "Envelope.WARC-Header-Metadata.WARC-Target-URI";
		tupleArray[1] = "Container.Filename";
		watFileLoader = new ArchiveJSONViewLoader(tupleArray);
		watFileLoader.prepareToRead(new ResourceRecordReader(), null);
		watFileLoader.getReader().initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return watFileLoader.getReader().nextKeyValue();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		this.currentTuple = watFileLoader.getNext();
		return new Text(currentTuple.get(0).toString());
	}

	@Override
	public Text getCurrentValue() throws IOException,
			InterruptedException {
		return new Text(currentTuple.get(1).toString());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return watFileLoader.getReader().getProgress();
	}

	@Override
	public void close() throws IOException {
		if(null != watFileLoader){
			watFileLoader.getReader().close();
		}
	}

}
