package org.ccindex.pbtree;

import java.io.IOException;
import java.util.List;

import org.ccindex.pbtree.utils.Stream;

public class PBTreeSequenceWriter extends PBTreeWriter{

	public PBTreeSequenceWriter(Stream stream, int blockSize,
			String terminator, String valueFormat) throws IOException {
		super(stream, blockSize, terminator, valueFormat);
	}
	
	public void packValue(String valueFormat, List<String> values){
		for(String value : values){
			this.packValue(value);
		}
	}
	

}
