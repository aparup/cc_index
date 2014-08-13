package org.ccindex.pbtree;

import java.io.IOException;

import org.ccindex.pbtree.utils.Stream;

/**
 * @author apbanerjee
 *
 */
public class PBTreeDictWriter extends PBTreeWriter{

	public PBTreeDictWriter(Stream stream, int blockSize, String terminator,
			String valueFormat) throws IOException {
		super(stream, blockSize, terminator, valueFormat);
		// TODO Auto-generated constructor stub
	}

}
