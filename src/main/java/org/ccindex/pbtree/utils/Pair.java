package org.ccindex.pbtree.utils;

public class Pair<L,R> {
	
	
	public Pair(L left, R right) {
		super();
		this.left = left;
		this.right = right;
	}

	public L getLeft() {
		return this.left;
	}

	public void setKey(L left) {
		this.left = left;
	}

	public R getRight() {
		return this.right;
	}

	public void setRight(R right) {
		this.right = right;
	}

	private L left;
	
	private R right;
	
	

}
