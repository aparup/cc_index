package org.ccindex.pbtree;

import java.io.IOException;

import org.ccindex.pbtree.utils.PrefixUtil;
import org.ccindex.pbtree.utils.Stream;
import org.ccindex.pbtree.utils.Struct;

public class PBTreeWriter implements IDelegate{

	private Stream stream;
	
	private int blockSize;
	
	private String terminator = "\0";
	
	private String valueFormat = "<0";
	
	private String lastKey = "";
	
	private DataWriter dataSegment;
	
	private IndexWriter indexSegment;
	
	private int DISK_BLOCK_SIZE=1024 * 4;
	
	public PBTreeWriter(Stream stream, int blockSize, String terminator , String valueFormat) throws IOException{
		this.stream = stream;
		assert terminator.length() == 1:"terminator must be of legth 1" ;
		this.blockSize = blockSize;
		this.terminator = terminator;
		this.valueFormat = valueFormat;
		this.indexSegment = new IndexWriter(stream,blockSize,terminator);
		this.dataSegment = new DataWriter(new TemporaryFile(),blockSize,terminator,this);
	}
	
	
	
	
	@Override
	public void onItemExceedsBlockSize(String key, String value) {
		throw new IllegalArgumentException("key '%s'  exceeds block size" + key);
	}

	@Override
	public byte[] packValue(String value) {
		return Struct.pack(this.valueFormat, value);
	}

	@Override
	public void onNewBlock(String key) throws IOException {
		String prefixKey = PrefixUtil.significant(this.lastKey, key);
		this.indexSegment.add(0, prefixKey);
	}
	
	public void add(String key, String value) throws IOException{
		this.dataSegment.add(key,value);
	    this.lastKey = key;
	}
	
	public void commit() throws IOException{
		Stream out = this.stream;
		this.indexSegment.finish();    
	    this.dataSegment.finish();
	    while(true){
	    	byte[] bytes = this.dataSegment.read(DISK_BLOCK_SIZE);
	    	if (new String(bytes) == ""){
	    		break;
	    	}
	    	else{
	    		out.write(bytes);
	    	}
	    }
	}
	
	public void close() throws IOException{
		this.commit();
	    this.stream.close();
	}
	
	
	
	private class TemporaryFile extends Stream{
		
	}

}
