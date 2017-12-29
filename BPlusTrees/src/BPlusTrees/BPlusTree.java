package hw3;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Stack;

import hw1.Field;
import hw1.IntField;
import hw1.RelationalOperator;

public class BPlusTree {
	
	private int degree;
	public ArrayList<Node> nodes;
    
    public BPlusTree(int degree) {
    	this.degree = degree;
    	this.nodes = new ArrayList<Node>();
    }
    
    public LeafNode search(Field f) {
    	//your code here
    	Node root = this.getRoot();
    	return treeSearch(f, root);
    }
    
    public LeafNode treeSearch(Field f, Node root){
    	if (root.isLeafNode()){
    		LeafNode searchNode = (LeafNode) root;
    		ArrayList<Entry> entries = searchNode.getEntries();
    		for (int i = 0; i < entries.size(); i++){
    			if (f.compare(RelationalOperator.EQ, entries.get(i).getField())){
    				return searchNode;
    			}
    		}
    		return null;
    	}
    	InnerNode first = (InnerNode) root;
    	ArrayList<Field> keys = first.getKeys();
    	int childNum = 0;
    	boolean foundIt = false;
    	for (int i = 0; i < keys.size(); i++){
    		if(!foundIt){
    			if (f.compare(RelationalOperator.LTE, keys.get(i))){
    				childNum = i;
    				foundIt = true;
    			}
    		}
    	}
    	if(!foundIt && f.compare(RelationalOperator.GTE, keys.get(keys.size() - 1))){
    			childNum = keys.size();
    		}
    	ArrayList<Node> children = first.getChildren();
    	return treeSearch(f, children.get(childNum));    	
    }
    
//    public InnerNode innerSearch(Field f) {
//    	//your code here
//    	Node root = this.getRoot();
//    	return treeInnerSearch(f, root);
//    }
//    
//    public InnerNode treeInnerSearch(Field f, Node root){
//    	if (root.isLeafNode()){
//    		return (InnerNode) root;
//    	}
//    	InnerNode first = (InnerNode) root;
//    	ArrayList<Field> keys = first.getKeys();
//    	int childNum = 0;
//    	boolean foundIt = false;
//    	for (int i = 0; i < degree; i++){
//    		if(!foundIt){
//    		if (f.compare(RelationalOperator.LTE, keys.get(i))){
//    			childNum = i;
//    			foundIt = true;
//    		}
//    		}
//    	}
//    	if(!foundIt && f.compare(RelationalOperator.GTE, keys.get(degree - 1))){
//    			childNum = degree;
//    		}
//    	ArrayList<Node> children = first.getChildren();
//    	return treeInnerSearch(f, children.get(childNum));    	
//    }
    
    public void insert(Entry e) {
    	if(nodes.size() == 0){
    		LeafNode root = new LeafNode(degree);
    		nodes.add(root);
    		root.setRoot();
    		root.entries.add(e);
    	}
    	Node root = this.getRoot();
    	Stack<Node> s = new Stack<Node>();
    	while(!root.isLeafNode()){
    		s.push(root);
    		InnerNode parentNode = (InnerNode) root;
    		int q = parentNode.getChildren().size();
    		boolean foundSmaller = false;
    		int numToAdd = parentNode.children.size()-1;
    		for(int i = 0; i < q-1; i++){//made it minus 1
    			if(!foundSmaller){
    				if(e.getField().compare(RelationalOperator.LTE, parentNode.getKeys().get(i))){
    					numToAdd = i;
    					foundSmaller = true;
    				}
    			}
    		}
    		root = parentNode.getChildren().get(numToAdd);
    	}
    	LeafNode correctNode = (LeafNode) root;
    	for(int i = 0; i < correctNode.getEntries().size(); i++){
    		if(e.getField().compare(RelationalOperator.EQ, correctNode.entries.get(i).getField())){
    			return;
    		}
    	}
    	
    	if(correctNode.getEntries().size() <= degree - 1){
    		ArrayList<Entry> newby = new ArrayList<Entry>(degree);
    		boolean foundSmaller = false;
    		int numToAdd = correctNode.getEntries().size();
    		for(int i = 0; i < correctNode.getEntries().size(); i++){
    			if(!foundSmaller){
    				if(e.getField().compare(RelationalOperator.LTE, correctNode.entries.get(i).getField())){
    					numToAdd = i;
    					foundSmaller = true;
    				}
    			}
    		}
    		for(int j = 0; j < numToAdd; j++){
    			newby.add(correctNode.entries.get(j));
    		}
    		newby.add(e);
    		for(int k = numToAdd; k < correctNode.entries.size(); k++){
    			newby.add(correctNode.entries.get(k));
    		}
    		correctNode.entries = newby;
    		if(!correctNode.isRoot){
    			InnerNode nodeToUpdate = (InnerNode) correctNode.parent;
    			int infinity = 0;
    			while(infinity < 10){
    				if(nodeToUpdate.isRoot){
    					nodeToUpdate.updateKeys();
    					break;
    				}
    				nodeToUpdate.updateKeys();
    				nodeToUpdate = (InnerNode) nodeToUpdate.parent;

    			}
    		}
    		//    		for(int i = 0; i < nodes.size(); i++){
    		//    			if(!nodes.get(i).isLeafNode()){
    		//    				InnerNode updatedNode = (InnerNode) nodes.get(i);
    		//    				updatedNode.updateKeys();
    		//    			}
    		//    		}
    	}
    	else{
    		LeafNode tempNode = new LeafNode(degree + 1);
    		tempNode.entries = correctNode.entries;
    		ArrayList<Entry> newby = new ArrayList<Entry>(degree + 1);
    		boolean foundSmaller = false;
    		int numToAdd = tempNode.entries.size();
    		for(int i = 0; i < tempNode.entries.size(); i++){
    			if(!foundSmaller){
    				if(e.getField().compare(RelationalOperator.LTE, tempNode.entries.get(i).getField())){
    					numToAdd = i;
    					foundSmaller = true;
    				}
    			}
    		}
    		for(int j = 0; j < numToAdd; j++){
    			newby.add(tempNode.entries.get(j));
    		}
    		newby.add(e);
    		for(int k = numToAdd; k < tempNode.entries.size(); k++){
    			newby.add(tempNode.entries.get(k));
    		}
    		tempNode.entries = newby;
    		LeafNode newNode = new LeafNode(degree);
    		double jk = (double)(degree + 1)/2;
    		int j = (int)Math.ceil(jk);
    		ArrayList<Entry> leftEntries = new ArrayList<Entry>(degree);
    		ArrayList<Entry> rightEntries = new ArrayList<Entry>(degree);
    		for(int k = 0; k < j; k++){
    			leftEntries.add(tempNode.getEntries().get(k));
    		}
    		for(int l = j; l < tempNode.getEntries().size(); l++){
    			rightEntries.add(tempNode.getEntries().get(l));
    		}
    		correctNode.entries = leftEntries;
    		newNode.entries = rightEntries;
    		boolean finished = false;
    		boolean checkmark = false;
    		InnerNode upperNode = new InnerNode(degree);
    		InnerNode newSplitNode = new InnerNode(degree);
    		while(!finished){;
    			if(s.isEmpty()){
    				if(checkmark){
    					InnerNode rootNode = new InnerNode(degree);
    					rootNode.setRoot();	
    					upperNode.setNotRoot();
    					rootNode.children.add(upperNode);
        				rootNode.children.add(newSplitNode);
        				upperNode.parent= rootNode;
        				newSplitNode.parent = rootNode;
        				nodes.add(rootNode);
        				rootNode.updateKeys();
        				finished = true;
    				}
    				else{
    				InnerNode rootNode = new InnerNode(degree);
    				rootNode.setRoot();
    				correctNode.setNotRoot();
    				nodes.get(0).setNotRoot();
    				rootNode.children.add(correctNode);
    				rootNode.children.add(newNode);
    				correctNode.parent= rootNode;
    				newNode.parent = rootNode;
    				rootNode.updateKeys();
    				finished = true;
    				nodes.add(rootNode);
    				nodes.add(newNode);
    				}
    				//    			for(int i = 0; i < nodes.size(); i++){
    				//        			if(!nodes.get(i).isLeafNode()){
    				//        				InnerNode updatedNode = (InnerNode) nodes.get(i);
    				//        				updatedNode.updateKeys();
    				//        			}
    				//        		}
    			}
    			else{
    				upperNode = (InnerNode)s.pop();
    				if(upperNode.getChildren().size() <= degree){
    					boolean foundPlace = false;
    					int numberToAdd = upperNode.getChildren().size(); //might be minus 1
    					for(int i = 0; i < upperNode.getChildren().size(); i ++){//made it minus 1
    						if(!foundPlace){
    							if(e.getField().compare(RelationalOperator.LTE, upperNode.keys.get(i))){
    								numberToAdd = i;
    								foundPlace = true;
    							}
    						}
    					}
    					ArrayList<Node> newChildren = new ArrayList<Node>(degree+1);
    					for(int m = 0; m < numberToAdd; m++){
    						newChildren.add(upperNode.getChildren().get(m));
    					}
    					newChildren.add(correctNode);
    					newChildren.add(newNode);
    					correctNode.parent= upperNode;
    					newNode.parent = upperNode;
    					for(int n =  numberToAdd+1; n < upperNode.children.size(); n++){
    						newChildren.add(upperNode.getChildren().get(n));
    					}
    					upperNode.children = newChildren;
    					nodes.add(newNode);
    					upperNode.updateKeys();
    					if(!upperNode.isRoot){
    						InnerNode nodeToUpdate = (InnerNode) upperNode.parent;
    						int infinity = 0;
    						while(infinity < 10){
    							if(nodeToUpdate.isRoot){
    								nodeToUpdate.updateKeys();
    								break;
    							}
    							nodeToUpdate.updateKeys();
    							nodeToUpdate = (InnerNode) nodeToUpdate.parent;

    						}
    					}
    					finished = true;
    				}
    				else{
    					checkmark = true;
    					InnerNode temporaryNode = new InnerNode(degree+2);
    					temporaryNode.children = upperNode.children;
    					boolean foundPlace = false;
    					int numberToAdd = upperNode.getChildren().size();
    					for(int i = 0; i < upperNode.getChildren().size()-1; i ++){//made it minus 1
    						if(!foundPlace){
    							if(e.getField().compare(RelationalOperator.LTE, upperNode.keys.get(i))){
    								numberToAdd = i;
    								foundPlace = true;
    							}
    						}
    					}
    					ArrayList<Node> newChildren = new ArrayList<Node>(degree+1);
    					for(int m = 0; m < numberToAdd; m++){
    						newChildren.add(upperNode.getChildren().get(m));
    					}
    					newChildren.add(correctNode);
    					newChildren.add(newNode);
    					correctNode.parent= temporaryNode;
    					newNode.parent = temporaryNode;
    					for(int n =  numberToAdd + 1; n < upperNode.children.size(); n++){
    						newChildren.add(upperNode.getChildren().get(n));
    					}
    					temporaryNode.children = newChildren;
//    					temporaryNode.updateKeys();
    					newSplitNode = new InnerNode(degree);
    					double wk = (double) (degree + 2)/2;
    					int w = (int) Math.ceil(wk);
    					ArrayList<Node> leftChildren = new ArrayList<Node>(degree+1);
    					ArrayList<Node> rightChildren = new ArrayList<Node>(degree+1);
    					for(int a = 0; a < w; a++){
    						leftChildren.add(temporaryNode.children.get(a));
    					}
    					for(int b = w; b < temporaryNode.children.size(); b++){
    						rightChildren.add(temporaryNode.children.get(b));
    					}
    				
    					upperNode.children = leftChildren;
    					newSplitNode.children = rightChildren;
    					for(int i = 0; i<upperNode.children.size();i++){
    						LeafNode newOnes = (LeafNode) upperNode.children.get(i);
    						newOnes.parent = upperNode;
    					}
    					for(int i = 0; i<newSplitNode.children.size();i++){
    						LeafNode newOnes2 = (LeafNode) newSplitNode.children.get(i);
    						newOnes2.parent = newSplitNode;
    					}
    					upperNode.updateKeys();
    					newSplitNode.updateKeys();
//    					newSplitNode = (InnerNode) upperNode.getParent();
    					nodes.add(newSplitNode);
//    					if(!upperNode.isRoot){
//    						InnerNode nodeToUpdate = (InnerNode) upperNode.parent;
//    						int infinity = 0;
//    						while(infinity < 10){
//    							if(nodeToUpdate.isRoot){
//    								nodeToUpdate.updateKeys();
//    								break;
//    							}
//    							nodeToUpdate.updateKeys();
//    							nodeToUpdate = (InnerNode) nodeToUpdate.parent;
//
//    						}
//    					}
    				}
    			}
    		}
    	}
    }
   
    
    public void delete(Entry e) {
    	//your code here
    	LeafNode deleteEntryNode = search(e.getField());
    	if(deleteEntryNode == null) {
    		return;
    	}
    	ArrayList<Node> siblings = new ArrayList<Node>();
    	InnerNode deleteNodeParent = new InnerNode(degree);
    	LeafNode leftSibling = new LeafNode(degree);
    	LeafNode rightSibling = new LeafNode(degree);
    	if(!deleteEntryNode.isRoot){
    		deleteNodeParent = (InnerNode) deleteEntryNode.parent;
    	}
    	if(deleteNodeParent.children != null) {
    		siblings = deleteNodeParent.children; 
    	}
    	
    	int siblingSize = siblings.size();
    	
    	int siblingIndex = siblings.indexOf(deleteEntryNode);
    	if(siblingSize > 1 && siblingIndex > 0) {
    		leftSibling = (LeafNode) siblings.get(siblingIndex - 1); 
    	
    	
    	}

    	if(siblingSize > siblingIndex + 1 && siblingIndex < siblingSize - 1) {
    		rightSibling = (LeafNode) siblings.get(siblingIndex + 1); 
    	
    	
    	}
    	LeafNode siblingToBorrowFrom = leftSibling;
    	int place = deleteEntryNode.entries.size() - 1;
    	boolean foundIt = false;
    	for(int i = 0; i < deleteEntryNode.entries.size();i++){
    		if(!foundIt){
    			if(e.getField().compare(RelationalOperator.EQ, deleteEntryNode.entries.get(i).getField())){
    				place = i;
    				foundIt = true;
    			}
    		}
    	}
    	ArrayList<Entry> newOnes = new ArrayList<Entry>(degree);
    	for(int i = 0; i < place;i++){
    		newOnes.add(deleteEntryNode.entries.get(i));
    	}
    	for(int i = place + 1; i < deleteEntryNode.entries.size();i++){
    		newOnes.add(deleteEntryNode.entries.get(i));
    	}
    	deleteEntryNode.entries = newOnes;
    	
    	
    	if(deleteEntryNode.entries.size() >= Math.ceil((double) (degree/2))) {
    		if(!deleteEntryNode.isRoot){
    			InnerNode nodeToUpdate = (InnerNode) deleteEntryNode.parent;
    			int infinity = 0;
    			while(infinity < 10){
    				if(nodeToUpdate.isRoot){
    					nodeToUpdate.updateKeys();
    					break;
    				}
    				nodeToUpdate.updateKeys();
    				nodeToUpdate = (InnerNode) nodeToUpdate.parent;

    			}
    		}
    		return;
    	}
    	else {
    		// take an entry from left sibling
    		
    		
    		boolean right = false;
    		if(leftSibling == null || leftSibling.entries.size() < Math.ceil((double) (degree/2))) {
    			siblingToBorrowFrom = rightSibling;
    			right = true;
    		}
    		
    		if(siblingToBorrowFrom != null && siblingToBorrowFrom.entries.size()-1 >= Math.ceil((double) (degree/2))){
    			if(!right) {
    				ArrayList<Entry> newEntries = new ArrayList<Entry>();
    				Entry lastEntry = siblingToBorrowFrom.entries.get(siblingToBorrowFrom.entries.size() - 1);
    		
    				
    				siblingToBorrowFrom.entries.remove(lastEntry);
    				newEntries.add(lastEntry);
    				for(int i = 0; i < deleteEntryNode.entries.size(); i++) {
    					newEntries.add(deleteEntryNode.entries.get(i));
    				}
    				deleteEntryNode.entries = newEntries;
    				deleteNodeParent.updateKeys();
    				if(!deleteEntryNode.isRoot){
    	    			InnerNode nodeToUpdate = (InnerNode) deleteEntryNode.parent;
    	    			int infinity = 0;
    	    			while(infinity < 10){
    	    				if(nodeToUpdate.isRoot){
    	    					nodeToUpdate.updateKeys();
    	    					break;
    	    				}
    	    				nodeToUpdate.updateKeys();
    	    				nodeToUpdate = (InnerNode) nodeToUpdate.parent;

    	    			}
    	    		}
    			}
    			else {
    				Entry lastEntry = siblingToBorrowFrom.entries.get(0);
    				siblingToBorrowFrom.entries.remove(lastEntry);
    				deleteEntryNode.entries.add(lastEntry);
    				deleteNodeParent.updateKeys();
    				if(!deleteEntryNode.isRoot){
    	    			InnerNode nodeToUpdate = (InnerNode) deleteEntryNode.parent;
    	    			int infinity = 0;
    	    			while(infinity < 10){
    	    				if(nodeToUpdate.isRoot){
    	    					nodeToUpdate.updateKeys();
    	    					break;
    	    				}
    	    				nodeToUpdate.updateKeys();
    	    				nodeToUpdate = (InnerNode) nodeToUpdate.parent;

    	    			}
    	    		}
    			}
    			return;
    		}
    		else {
    			// merge node with sibling
    			
    			int deleteNodeSize = deleteEntryNode.entries.size();
    			if(!right) {
    				for(int i = 0; i < deleteNodeSize; i++) {
    					siblingToBorrowFrom.entries.add(deleteEntryNode.entries.get(i));
    				} 
    			}
    			// merging with right
    			else {
    				ArrayList<Entry> newEntries = new ArrayList<Entry>();
    				for(int i = 0; i < deleteNodeSize; i++) {
    				newEntries.add(deleteEntryNode.entries.get(i));
    				}
    				for(int i = 0; i < siblingToBorrowFrom.entries.size(); i++){
    				newEntries.add(siblingToBorrowFrom.entries.get(i));
    				}
    				siblingToBorrowFrom.entries = newEntries;
    			}
    			deleteNodeParent.children.remove(deleteEntryNode);
    			deleteNodeParent.updateKeys();
    			if(!deleteEntryNode.isRoot){
	    			InnerNode nodeToUpdate = (InnerNode) deleteEntryNode.parent;
	    			int infinity = 0;
	    			while(infinity < 10){
	    				if(nodeToUpdate.isRoot){
	    					nodeToUpdate.updateKeys();
	    					break;
	    				}
	    				nodeToUpdate.updateKeys();
	    				nodeToUpdate = (InnerNode) nodeToUpdate.parent;

	    			}
	    		}
    			boolean finished = false;

    			// merge parents if needed
    			while(!finished) {
    				// parents don't need to merge
    				if(deleteNodeParent.children.size() >= Math.ceil((double) (degree+1)/2)) {
    					finished = true;
    					break;
    				}
    				
    				InnerNode deleteNodeGrandparent = (InnerNode) deleteNodeParent.parent;
    				if(deleteNodeParent.isRoot){
						LeafNode newRoot = new LeafNode(degree);
						ArrayList<Entry> finalEntries = new ArrayList<Entry>(degree);
						for(int i = 0; i < deleteNodeParent.children.size(); i++){
							LeafNode chillun = (LeafNode)deleteNodeParent.children.get(i);
							
							for(int j = 0; j < chillun.entries.size(); j++){
							finalEntries.add(chillun.entries.get(j));
						}
						}
						newRoot.entries = finalEntries;
						
						deleteNodeParent.setNotRoot();
						nodes.add(newRoot);
						newRoot.setRoot();
						
						finished = true;
						break;
					}
    				ArrayList<Node> parentSiblings = deleteNodeGrandparent.children;
    				
    				int index = 0;
    				
    				
    				boolean foundPlace = false;
    				InnerNode siblingTester = new InnerNode(degree); 
					for(int i = parentSiblings.size()-1; i == 0; i --){
						if(!foundPlace){
							siblingTester = (InnerNode) parentSiblings.get(i);
							if(e.getField().compare(RelationalOperator.GTE, siblingTester.keys.get(0))){
								index = i;
								foundPlace = true;
							}
						}
					}
					
					boolean noLeftSibling = false;
					boolean noRightSibling = false;
					if(index == 0){
						noLeftSibling = true;
					}
					if (index == parentSiblings.size()-1){
						noRightSibling = true;
					}
					InnerNode parentLeftSibling = new InnerNode(degree);
					InnerNode parentRightSibling = new InnerNode(degree);
					
					if(!noLeftSibling){
    				parentLeftSibling = (InnerNode) parentSiblings.get(index-1);
					}
					if(!noRightSibling){
    				parentRightSibling = (InnerNode) parentSiblings.get(index+1);
					}
    				InnerNode parentSiblingToBorrowFrom = parentLeftSibling;
    				int parentNodeSize = deleteNodeParent.children.size();
    				InnerNode parentNodeParent = (InnerNode) deleteNodeParent.parent; 
    				boolean wasRight = false;
    				if(parentLeftSibling == null || parentLeftSibling.keys.size() < Math.ceil((double) (degree/2)) ) {
    					parentSiblingToBorrowFrom = parentRightSibling;
    					wasRight = true;
    				}
    				// merge parents
    				if(parentSiblingToBorrowFrom != null && siblingToBorrowFrom.entries.size()-1 >= Math.ceil((double) (degree/2))){
    				if(!wasRight){
    				for(int i = 0; i < parentNodeSize; i++) {
    					parentSiblingToBorrowFrom.children.add(deleteNodeParent.children.get(i));
    				}
    				}
    				else{
    					ArrayList<Node> newChildren = new ArrayList<Node>(degree+1);
    					newChildren.addAll(deleteNodeParent.children);
    					newChildren.addAll(parentSiblingToBorrowFrom.children);
    					parentSiblingToBorrowFrom.children = newChildren;
    				}
    				parentNodeParent.children.remove(deleteNodeParent);
    				parentSiblingToBorrowFrom.updateKeys();
    				parentNodeParent.updateKeys();
    				deleteNodeParent = (InnerNode) deleteNodeParent.parent;
    				}
    				else{
    					
    					ArrayList<Node> newMergedChildren = new ArrayList<Node>(degree+1);
    					
    					for(int i = 0; i < parentSiblings.size(); i++){
    						if(i == index){
    							for(int j = 0; j < deleteNodeParent.children.size(); j++){
    								LeafNode newChild = (LeafNode) deleteNodeParent.children.get(j);
    								newChild.parent = deleteNodeGrandparent;
    								newMergedChildren.add(newChild);
    							}
    						}
    						else{
    						InnerNode mergeChild = (InnerNode) parentSiblings.get(i);
    						LeafNode mergedChild = (LeafNode) merge(mergeChild);
    						mergedChild.parent = deleteNodeGrandparent;
    						newMergedChildren.add(mergedChild);
    						}
    					}
    					deleteNodeGrandparent.children = newMergedChildren;
    					deleteNodeGrandparent.updateKeys();
    					finished = true;
    				}
    			}
    		}
    	}
    }
    
    public LeafNode merge(InnerNode mergeChild){
    	LeafNode child = new LeafNode(degree);
    	LeafNode newlyMergedChild = new LeafNode(degree);
    	for(int i = 0; i < mergeChild.children.size(); i++){
    		child = (LeafNode) mergeChild.children.get(i);
    		for(int j = 0; j < child.entries.size(); j++){
    			newlyMergedChild.entries.add(child.entries.get(j));
    		}
    	}
    	return newlyMergedChild;
    }
    
    public Node getRoot() {
    	//your code here
    	if(nodes.size() > 0){
    		if(nodes.size() > 1){
    			for(int i = 0; i < nodes.size(); i ++){
    				if(nodes.get(i).isRoot()){
    					return nodes.get(i);
    				}
    			}
    		}
    	return nodes.get(0);
    	}
    	else{
    		return null;
    	}
    }
}
