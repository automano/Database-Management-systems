package hw3;

import java.util.ArrayList;

public interface Node {
	
	
	public int getDegree();
	public boolean isLeafNode();
	public boolean isRoot();
	public void setRoot();
	public void setNotRoot();
	public boolean isRoot = false;
	public ArrayList<Node> children = new ArrayList<Node>();
	
}
