package hw3;

import java.util.ArrayList;

public class LeafNode implements Node {
	
	private int degree;
	public ArrayList<Entry> entries;
	public Node parent;
	public boolean isRoot;
	public ArrayList<Node> children;
	
	
	public LeafNode(int degree) {
		this.degree = degree;
		this.entries = new ArrayList<Entry>(degree);
		this.parent = parent;
		this.isRoot = false;
		this.children = new ArrayList<Node>(0);
	}
	
	public ArrayList<Entry> getEntries() {
		//your code here
		return this.entries;
	}

	public int getDegree() {
		//your code here
		return this.degree;
	}
	
	public boolean isLeafNode() {
		return true;
	}
	
	public Node getParent(){
		if(!this.isRoot()){
		return this.parent;
		}
		else{
			return null;
		}
	}
	
	public void setRoot(){
		this.isRoot = true;
	}
	
	public void setNotRoot(){
		this.isRoot = false;
	}
	
	public boolean isRoot(){
		return this.isRoot;
	}
	
	public ArrayList<Node> getChildren() {
		//your code here
		return this.children;
	}

}