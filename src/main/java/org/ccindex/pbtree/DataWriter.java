package org.ccindex.pbtree;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.lang.StringUtils;
import org.ccindex.pbtree.utils.ByteBuffer;
import org.ccindex.pbtree.utils.Stream;

public class DataWriter {
	
	
	private boolean finalized = false;
	private IDelegate delegate = null;
	
	private int blockSize = 0;
	
	private int remaining = 0;
	
	private String terminator = null;
	
	private int terminatorLenth = 0;
	
	private Stream stream;
	
	private ByteBuffer writeBuffer;
	
	
		    
	public DataWriter(Stream stream,int blockSize, String terminator,  IDelegate delegate){
		this.delegate = delegate;
		this.blockSize = blockSize;
		this.remaining = blockSize;
		this.terminator = terminator;
		this.terminatorLenth = terminator.length();
		this.stream = stream;
		this.writeBuffer = new ByteBuffer();
	}
	
	public void add(String key, String value) throws IOException{
		byte[] packet = this.delegate.packValue(value);
	    int size = key.length() + this.terminatorLenth + packet.length;
	    if(size > this.blockSize)
	    {
	    	this.delegate.onItemExceedsBlockSize(key, value);
	    }
	    else if(size > this.remaining){
	    	this.writeBuffer.extend(StringUtils.repeat(terminator, this.remaining).getBytes());
		    this.stream.write(this.writeBuffer.getBytes());
		    this.writeBuffer.clear();
		    this.remaining = this.blockSize;
		    this.delegate.onNewBlock(key);
	    }
	    this.writeBuffer.extend(key.getBytes());
	    this.writeBuffer.extend(this.terminator.getBytes());
	    this.writeBuffer.extend(packet);
	}
	
	public void close() throws IOException{
		if(!this.finalized){
			this.finish();
		}
		this.stream.flush();
		this.stream.close();
	}
	
	public void finish() throws IOException{
		if(null != this.writeBuffer && !this.writeBuffer.isEmpty()){
			this.writeBuffer.extend(StringUtils.repeat(terminator, this.remaining).getBytes());
			this.stream.write(this.writeBuffer.getBytes());
		}
		if(null != this.writeBuffer){
			this.writeBuffer.clear();
		}
		this.delegate = null;
		//TODO
		//this.stream.seek(0);
		this.finalized = true;
	}
	
	public byte[] read(int blockSize){
		//TODO
		return null;
	}

}
