// John Xiahou
// Deeksha Chaudhary

package hw1;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types;
	private String[] fields;
	private int size;
	
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	//your code here
    	types = typeAr;
    	fields = fieldAr;
    	size = 0;
    	for(Type type : typeAr) {
    		if(type==Type.INT) {
    			size += 4;
    		}
    		else if(type==Type.STRING) {
    			size += 129;
    		}
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        //your code here
    	return fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        //your code here
    	if(i < 0 && i >= fields.length) {
    		throw new NoSuchElementException("Field "+i+" doesn't exist.");
    	}
    	return fields[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        //your code here
    	boolean found = false;
    	int index = -1;
    	for(int i = 0; i < fields.length; i++) {
    		if(fields[i].equals(name)) {
    			found = true;
    			index = i;
    		}
    	}
    	if(!found) {
    		throw new NoSuchElementException("Field with name "+name+" doesn't exist.");
    	}
    	else {
    		return index;
    	}
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        //your code here
    	if(i < 0 || i >= types.length) {
    		throw new NoSuchElementException("Field "+i+" doesn't exist.");
    	}
    	return types[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	//your code here
    	return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	//your code here
    	TupleDesc td = (TupleDesc) o;
    	if((td.getSize() == size) && (td.numFields() == this.numFields())){
    		for(int i = 0; i < this.numFields(); i++) {
    			if(types[i]!=td.getType(i)){
    				return false;
    			}
    		}
    	} else {
    		return false;
    	}
    	return true;
    }
    

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // throw new UnsupportedOperationException("unimplemented");
    	return Arrays.hashCode(types) * 31 +Arrays.hashCode(fields);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        //your code here
    	StringBuilder description = new StringBuilder();
    	for(int i=0; i < this.numFields(); i++) {
    		description.append(types[i].toString()+"("+fields[i]+")");
    	}
    	return description.toString();
    }
}
