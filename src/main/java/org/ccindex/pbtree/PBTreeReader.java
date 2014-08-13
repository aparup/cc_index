package org.ccindex.pbtree;

import java.io.IOException;
import java.util.Iterator;

import org.ccindex.pbtree.utils.Arrays;
import org.ccindex.pbtree.utils.MMap;
import org.ccindex.pbtree.utils.Pair;
import org.ccindex.pbtree.utils.Stream;
import org.ccindex.pbtree.utils.StringIO;
import org.ccindex.pbtree.utils.Struct;

import com.google.code.jyield.Generator;
import com.google.code.jyield.YieldUtils;
import com.google.code.jyield.Yieldable;

public class PBTreeReader {
	
	
	private MMap mmap;
	
	private String terminator = "\0";
	
	private String valueFormat = "<Q";
	
	private String headerFormat = "<II";
	
	private int headerSize = Struct.calcsize(headerFormat);
	
	private int valueSize = Struct.calcsize(valueFormat);
	
	private int blockSize;
	
	private int indexBlockSize;
	
	public PBTreeReader(MMap mmap, String terminator, String valueFormat) {
		super();
		this.mmap = mmap;
		this.terminator = terminator;
		this.valueFormat = valueFormat;
		this.blockSize   = this.fetchHeader();
		this.indexBlockSize = this.fetchHeader();
	}

	public static Iterable<Pair<Integer,String>> parse(Stream stream,int blockSize) throws IOException{
		while (true){
			String block = stream.read(blockSize);
			if (block == ""){
				break;
			}
			else{
				return YieldUtils.toArrayList(new IndexBlockReader(block));
			}
		}
		return null;
	}
	
	public void close(){
		this.mmap.close();
	}
	
	public byte[] fetch(int start, int end){
		return this.mmap.subMMap(start, end);
	}
	
	public int fetchHeader(){
		return Struct.unpack(this.headerFormat, this.fetch(0, this.headerSize));
	}
	
	public int blockOffset(int blockNumber){
	    return this.headerSize + (this.blockSize*blockNumber);
	}
	
	public IndexBlockReader block(int blockNumber){
		//Returns the block for given block number 
		int offset = this.blockOffset(blockNumber);
		String block = new String(this.fetch(offset, offset+this.blockSize));
		return new IndexBlockReader(block);
	}
	
	public int countLevels(){
		/* 
		 * Return the number of 'levels' in the index. This number represents
	    how many seeks have to be preformed before finding
	    the starting data block
		 * 
		 */
		int blockNumber = 0;
		IndexBlockReader block = this.block(blockNumber);
		int levels = 1;
		while(true){
			StringIO buffer = new StringIO(block.data());
			int nextBlockNumber = block.readOffset(buffer);
			if (nextBlockNumber < this.indexBlockSize){
				blockNumber = nextBlockNumber;
				block = this.block(blockNumber);
				levels = levels + 1;
			}
			else{
				return levels;
			}
		}
		
	}
	
	public int findStartingDataBlock(String key){
		//Return the number of the first data block where the key should be found.
		int blockNumber = 0;
		IndexBlockReader block = this.block(blockNumber);
		while (true){
			int nextBlockNumber = block.find(key);
			if(nextBlockNumber < this.indexBlockSize){
				blockNumber = nextBlockNumber;
				block = this.block(blockNumber);
			}
			else{
				//it's the start of the data segments,
				return nextBlockNumber;
			}
		}
	}
		
	public int expected_location(String key){
		/*
		 * Given a key, return the expected starting location for the key in the data
	    	segment. The return value is only the location  where the key "should" be
	    	not neccesarily where it is. This enables range queries
		 */
		if (key.equals("")){
			int startBlock = this.indexBlockSize;
			return this.blockOffset(startBlock);
		}
		int startingBlock = this.findStartingDataBlock(key);
		int offset = this.blockOffset(startingBlock);
		byte[] data = this.fetch(offset, offset+this.blockSize);
		// linear scan through the block, looking for the position of the stored key
	   // that is greater than the given key
		int start = 0;
		while(true){
			int pos = Arrays.find(data, this.terminator.getBytes(), start);
			if (pos == -1){
				return data.length;
			}
			byte[] stored = Arrays.subArray(data, start, pos);
			if (key.compareTo(new String(stored)) >= 0){
				return start + offset;
			}
			else{
				start = pos + 1 + this.valueSize;
			}
		}
	}
	
	public Generator<byte[]> blockIter(final int blockNumber){
		//Iterate over blocks starting with the given block_number
		Generator<byte[]> generator = new Generator<byte[]>(){
			@Override
			public void generate(Yieldable<byte[]> yieldable) {
				while(true){
					int offset = PBTreeReader.this.blockOffset(blockNumber);
					byte[] block = PBTreeReader.this.mmap.subMMap(offset, offset + PBTreeReader.this.blockSize);
					if (null != block && block.length != 0){
						yieldable.yield(block);
					}
					else{
						break;
					}
				}
			}
			
		};
		return generator;
	}
	
	public void dataIter(DataBlock block){
		DataBlockReader reader = new DataBlockReader(block,this.terminator,this.valueSize);
		for(Pair<String,String> pari : YieldUtils.toIterable(reader)){
			
		}
	}
	

	
}