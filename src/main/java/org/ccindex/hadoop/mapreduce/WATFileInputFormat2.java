package org.ccindex.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class WATFileInputFormat2 extends FileInputFormat<Text,Text>{

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}
	
	private static final String WAT_SUFFIX = "wat.gz";

	@Override
	protected FileStatus[] listStatus(JobConf job) throws IOException {
		return (FileStatus[])filterInputCandidates(super.listStatus(job)).toArray();
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		return new WATFileRecordReader2();
	}
	
	private static List<FileStatus> filterInputCandidates(FileStatus[] filestatusList) {
		// allocate new array
		ArrayList<FileStatus> listOut = new ArrayList<FileStatus>(
				filestatusList.length);
		// walk list removing invalid entries
		for(FileStatus fileStatus : filestatusList){
			Path pathAtIndex = fileStatus.getPath();
			if (pathAtIndex.getName().endsWith(WAT_SUFFIX)) {
				// add to final list
				listOut.add(fileStatus);
			}
		}
		return listOut;

	}

}
