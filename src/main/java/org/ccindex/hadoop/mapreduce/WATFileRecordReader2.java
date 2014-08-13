package org.ccindex.hadoop.mapreduce;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.archive.hadoop.ArchiveJSONViewLoader;
import org.archive.hadoop.ResourceRecordReader;

public class WATFileRecordReader2 implements RecordReader<Text,Text>{

	
	ArchiveJSONViewLoader watFileLoader;
	
	Tuple currentTuple;
	
	private void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		String[] tupleArray = new String[2];
		tupleArray[0] = "Envelope.WARC-Header-Metadata.WARC-Target-URI";
		tupleArray[1] = "Container.Filename";
		watFileLoader = new ArchiveJSONViewLoader(tupleArray);
		watFileLoader.prepareToRead(new ResourceRecordReader(), null);
		watFileLoader.getReader().initialize(split, context);
	}
	
	@Override
	public boolean next(Text key, Text value) throws IOException {
		try {
			return watFileLoader.getReader().nextKeyValue();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Text createKey() {
		try {
			this.currentTuple = watFileLoader.getNext();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			return new Text(currentTuple.get(0).toString());
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Text createValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
