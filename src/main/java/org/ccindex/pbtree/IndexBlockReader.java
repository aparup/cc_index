package org.ccindex.pbtree;

import java.util.ArrayList;
import java.util.List;

import org.ccindex.pbtree.utils.ByteBuffer;
import org.ccindex.pbtree.utils.Collections;
import org.ccindex.pbtree.utils.Pair;
import org.ccindex.pbtree.utils.StringIO;
import org.ccindex.pbtree.utils.Struct;

import com.google.code.jyield.Generator;
import com.google.code.jyield.YieldUtils;
import com.google.code.jyield.Yieldable;

public class IndexBlockReader implements Generator<Pair<Integer,String>>{

	private String data;
	
	private static final String OFFSET_FMT = "<I";
			
	private static final int OFFSET_SIZE = Struct.calcsize(OFFSET_FMT);
	
	public IndexBlockReader(String data){
		this.data = data;
	}
	
	public String data(){
		return this.data;
	}
	
	@Override
	public void generate(Yieldable<Pair<Integer, String>> yieldable) {
		// TODO Auto-generated method stub
		boolean endOfBlock = false;
		StringIO buffer = new StringIO(this.data);
		while (!endOfBlock) {
			int offset = this.readOffset(buffer);
			String key = this.readKey(buffer);
			if(key == ""){
				endOfBlock = true;
			}
			else if(key == "\0"){
				endOfBlock = true;
				key = key + this.readRestOfBlock(buffer);
			}
			yieldable.yield(new Pair<Integer,String>(offset,key));
		}

	}
	
	public int find(String key){
		List<Integer> pointers = new ArrayList<Integer>();
		List<String>  prefixes = new ArrayList<String>();
		
		for (Pair<Integer,String> pair : YieldUtils.toIterable(this)){
			pointers.add(pair.getLeft());
			prefixes.add(pair.getRight());
		}
		
		prefixes.remove(prefixes.size() - 1);
	    
	    int index = Collections.bisect(prefixes, key);
	    return pointers.get(index);
	}
	
	public int readOffset(StringIO buffer){
		byte[] bytes = buffer.read(OFFSET_SIZE);
	    // bytes should never be empty when reading an offset if so the file
	    // corrupt
	    return Struct.unpack(OFFSET_FMT, bytes);
	}
	
	public String readKey(StringIO buffer){
		ByteBuffer byteBuffer = new ByteBuffer();
		while (true){
			char c = buffer.readChar(1);
			if (c == ' '){
				if(byteBuffer.length() == 1){
					throw new  RuntimeException("EOF found when string was expected");
				}
				else{
					break;
				}
			}
			else if(c == '\0'){
				byteBuffer.append(c);
				break;
			}
			else{
				byteBuffer.append(c);
			}
		}
		return byteBuffer.toString();
	}
	
	public String readRestOfBlock(StringIO buffer){
		return new String(buffer.read());
	}

}
