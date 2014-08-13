package org.ccindex.pbtree;

import org.ccindex.pbtree.utils.Pair;

import com.google.code.jyield.Generator;
import com.google.code.jyield.Yieldable;

public class DataBlockReader implements Generator<Pair<String,String>>{
	
	public DataBlockReader(DataBlock block, String terminator, int valueSize) {
		super();
		this.block = block;
		this.terminator = terminator;
		this.valueSize = valueSize;
	}

	private DataBlock block;
	
	private String terminator = "\0";
	
	private int valueSize;

	@Override
	public void generate(Yieldable<Pair<String,String>> yieldable) {
		DataBlock block = this.block;
		int start = 0;
		while(true){
			int pos = block.find(this.terminator, start);
			if (pos == -1){
				break;
			}
			String key = block.getKey(pos);
			start=pos+1;
			if(key == ""){
				return;
			}
			else{
				String value = block.getValue(start+this.valueSize);
				start+=this.valueSize;
				yieldable.yield(new Pair<String,String>(key,value));
			}
		}
	}
}
