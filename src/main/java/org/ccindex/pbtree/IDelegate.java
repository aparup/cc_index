package org.ccindex.pbtree;

import java.io.IOException;

public interface IDelegate {
	
	public void onItemExceedsBlockSize(String key, String value);
	
	public byte[] packValue(String value);
	
	public void onNewBlock(String key) throws IOException;

}
