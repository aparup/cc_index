package org.ccindex.pbtree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.ccindex.pbtree.PBTreeReader.PBTreeNode;
import org.ccindex.pbtree.utils.Pair;
import org.ccindex.pbtree.utils.Stream;
import org.ccindex.pbtree.utils.Struct;

public class IndexWriter {
	
	private Stream stream;
	
	private int blockSize;
	
	private String terminator;
	
	private String pointerFormat = "<I";
	
	private static final String OFFSET_FMT = "<I";
	
	private int termSize = 0;
	
	private int pointerSize = 0;
	
	private ArrayList<Index> indexList = new ArrayList<Index>();
	
	
	public IndexWriter(Stream stream,int blockSize , String terminator, String pointerFormat) throws IOException{
		this.stream = stream;
		this.blockSize = blockSize;
		this.terminator = terminator;
		this.termSize = terminator.length();
		this.pointerFormat = pointerFormat;
		this.pointerSize = Struct.calcsize(pointerFormat);
		this.indexList = new ArrayList<Index>();
		this.pushIndex();
	}
	
	public IndexWriter(Stream stream,int blockSize , String terminator) throws IOException{
		this.stream = stream;
		this.blockSize = blockSize;
		this.terminator = terminator;
		this.termSize = terminator.length();
		this.pointerSize = Struct.calcsize(pointerFormat);
		this.indexList = new ArrayList<Index>();
		this.pushIndex();
	}
	
	public void add(int level, String key) throws IOException {
		int size = key.length() + this.termSize + this.pointerSize;
		Index index = this.indexList.get(level);
		Stream stream = index.getStream();
		int pointers = index.getPointers();
		int remaining = index.getRemaining();
		stream.write(Struct.pack(this.pointerFormat, pointers));
	    int next_level = level + 1;
	    if (next_level > (this.indexList.size() - 1)){
	        this.pushIndex();
	    }
	    this.add(next_level, key);
	    remaining = this.blockSize - this.pointerSize;
	    pointers = pointers + 1; 
	    stream.write(key.getBytes());
	    stream.write(this.terminator.getBytes());
	    stream.write(Struct.pack(this.pointerFormat, pointers));
	    remaining = remaining - size;
	    this.indexList.add(level,new Index(stream,pointers,remaining));
	}
	
	
	
	public void close() throws IOException{
		this.finish();
	}
	
	public void finish() throws IOException{
		Stream out = this.stream;
		int	blocksWritten = 0;
		out.write(Struct.pack(OFFSET_FMT, this.blockSize));
		List<Index> reverseList = this.indexList;
		Collections.reverse(reverseList);
		for (Index index : reverseList){
			//pad the stream
			Stream stream = index.getStream();
			stream.write(StringUtils.repeat(this.terminator, index.getRemaining()).getBytes());
		    int levelLength = stream.tell();
		    assert levelLength % this.blockSize == 0;
		    int blocksToWrite = (levelLength / this.blockSize);
		    stream.seek(0);
		    //loop through each pointer and key writing
		    for (Pair<Integer,String> pbTreeNode : PBTreeReader.parse(stream, this.blockSize)){
		    	out.write(Struct.pack(this.pointerFormat, pbTreeNode.getLeft()+blocksWritten+blocksToWrite));
		    	out.write(pbTreeNode.getRight().getBytes());
		    }
		    blocksWritten += blocksToWrite;
		    stream.close();
		}
	    //blocks in the index
	    out.write(Struct.pack(OFFSET_FMT, 0));
	}
	
	public void pushIndex() throws IOException{
		int maxSize = 20;
		Stream stream = new SpooledTemporaryFile(maxSize);
		int pointers = 0;
	    stream.write(Struct.pack(OFFSET_FMT,pointers));
	    this.indexList.add(new Index(stream,pointers,this.blockSize - this.pointerSize));
	}
	
	private class Index{
		
		private Stream stream;
		
		private int pointers;
		
		private int remaining;
		
		public Index (Stream stream, int pointers, int remaining){
			this.stream = stream;
			this.pointers = pointers;
			this.remaining = remaining;
		}
		public Stream getStream(){
			return this.stream;
		}
		
		public int getPointers(){
			return this.pointers;
		}
		
		public int getRemaining(){
			return this.remaining;
		}
	}
	
	private class SpooledTemporaryFile extends Stream{
		
		int maxSize;
		
		public SpooledTemporaryFile(int maxSize){
			this.maxSize = maxSize;
		}
		
		public void write(byte[] bytes) throws IOException{
			
		}
	}
}
