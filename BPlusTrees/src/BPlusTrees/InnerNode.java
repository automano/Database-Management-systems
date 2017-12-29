package hw3;

import java.util.ArrayList;

import hw1.Field;
import hw1.RelationalOperator;

public class InnerNode implements Node {
	
	private int degree;
	public ArrayList<Field> keys;
	public ArrayList<Node> children;
	public Node parent;
	public boolean isRoot;
	
	public InnerNode(int degree) {
		this.degree = degree;
		this.children = new ArrayList<Node>(degree + 1);
		this.keys = new ArrayList<Field>(degree);
		this.parent = parent;
		this.isRoot = false;
	}
	
	public ArrayList<Field> getKeys() {
		//your code here
		return this.keys;
	}
	
	public void updateKeys() {
		//your code here
		ArrayList<Field> newKeys = new ArrayList<Field>(degree);
		if(children.get(0).isLeafNode()){
			if(this.children.size() == 1){
				LeafNode newNode = (LeafNode) children.get(0);
				int maxEntry = newNode.entries.size()-1;
				newKeys.add(newNode.getEntries().get(maxEntry).getField());
			}
			else{
			for(int i = 0; i < this.children.size()-1; i++){
				LeafNode newNode = (LeafNode) children.get(i);
				Field first = newNode.getEntries().get(0).getField();
				for(int j = 0; j < newNode.getEntries().size(); j++){
					if(first.compare(RelationalOperator.LTE, newNode.getEntries().get(j).getField())){
						first = newNode.getEntries().get(j).getField();
					}
				}
				newKeys.add(first);
			}
			}
		}
		else{
			for(int i = 0; i < this.children.size()-1; i++){
				InnerNode newNode = (InnerNode) children.get(i);
				if(newNode.children.get(newNode.children.size()-1).isLeafNode()){
					LeafNode rightMost = (LeafNode) newNode.children.get(newNode.children.size()-1);
					Field first = rightMost.getEntries().get(rightMost.entries.size()-1).getField();
					newKeys.add(first);
				}
				else{
					while(!newNode.children.get(newNode.children.size()-1).isLeafNode()){
						newNode = (InnerNode) newNode.children.get(newNode.children.size()-1);
						if(newNode.children.get(newNode.children.size()-1).isLeafNode()){
							LeafNode rightMost = (LeafNode) newNode.children.get(newNode.children.size()-1);
							Field first = rightMost.getEntries().get(rightMost.entries.size()-1).getField();
							newKeys.add(first);
						}
					}
				}
			}
		}
		this.keys = newKeys;
	}
	
	public ArrayList<Node> getChildren() {
		//your code here
		return this.children;
	}

	public int getDegree() {
		//your code here
		return this.degree;
	}
	
	public boolean isLeafNode() {
		return false;
	}
	
	public Node getParent(){
		if(!this.isRoot()){
		return this.parent;
		}
		return null;
		
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

}