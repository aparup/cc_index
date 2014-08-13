package org.ccindex.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WATFileInputFormat extends FileInputFormat<Text, Text> {

	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		return filterInputCandidates(super.listStatus(job));
	}

	private static final String WAT_SUFFIX = "wat.gz";


	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new WATFileRecordReader();
	}

	private static List<FileStatus> filterInputCandidates(List<FileStatus> filestatusList) {
		// allocate new array
		ArrayList<FileStatus> listOut = new ArrayList<FileStatus>(
				filestatusList.size());
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
