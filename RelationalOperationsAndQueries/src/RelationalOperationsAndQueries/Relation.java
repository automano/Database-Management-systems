package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		ArrayList<Tuple> resultTuples = new ArrayList<Tuple>();

		Type fieldType = td.getType(field);
		if(fieldType == Type.STRING){
			for(int i = 0; i < tuples.size(); i++){
				Tuple t = tuples.get(i);
				StringField fieldName = new StringField(t.getField(field));
				if(fieldName.compare(op, operand)){
					resultTuples.add(t);
				}
			}
		}
		if(fieldType == Type.INT){
			for(int i = 0; i < tuples.size(); i++){
				Tuple t = tuples.get(i);
				IntField fieldName = new IntField(t.getField(field));
				if(fieldName.compare(op, operand)){
					resultTuples.add(t);
				}
			}
		}
		Relation result = new Relation(resultTuples, this.td);
		return result;
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		Type[] types = new Type[td.numFields()];
		types = td.returnTypeArray();
		String[] fieldNames = new String[td.numFields()];
		fieldNames = td.returnFieldsArray();
		for(int i = 0; i < fields.size(); i++){
			fieldNames[fields.get(i)] = names.get(i);
		}
		TupleDesc newTd = new TupleDesc(types, fieldNames);
		ArrayList<Tuple> resultTuples = new ArrayList<Tuple>();
		for(int j = 0; j < tuples.size(); j++){
			Tuple t = tuples.get(j);	
			Tuple tNew = new Tuple(newTd);
			tNew.setId(t.getId());
			tNew.setPid(t.getPid());
			for(int i = 0; i < td.numFields(); i++){
				tNew.setField(i, t.getField(i));
			}
			resultTuples.add(tNew);
		}
		Relation result = new Relation(resultTuples, newTd);
		return result;
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		ArrayList<Tuple> resultTuples = new ArrayList<Tuple>();
		Type[] types = new Type[fields.size()];
		String[] fieldNames = new String[fields.size()];
		for(int i = 0; i < fields.size(); i++){
			Integer index = fields.get(i);
			Type typeField = td.getType(index);
			types[i] = typeField;
			String fieldName = td.getFieldName(index);
			fieldNames[i] = fieldName;	
		}
		TupleDesc newTd = new TupleDesc(types, fieldNames);
		for(int j = 0; j < tuples.size(); j++){
			Tuple t = tuples.get(j);	
			Tuple tNew = new Tuple(newTd);

			tNew.setId(t.getId());
			tNew.setPid(t.getPid());
			for(int i = 0; i < fields.size(); i++){
				Integer index = fields.get(i);
				tNew.setField(i, t.getField(index));
			}
			resultTuples.add(tNew);
		}
		Relation result = new Relation(resultTuples, newTd);
		return result;
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		ArrayList<Tuple> resultTuples = new ArrayList<Tuple>();
		Type[] types = new Type[td.numFields() + other.getDesc().numFields()];
		String[] fieldNames = new String[td.numFields() + other.getDesc().numFields()];
		for(int n = 0; n < td.numFields(); n++){
			types[n] = td.getType(n);
			fieldNames[n] = td.getFieldName(n);
 		}
//		types = td.returnTypeArray();
//		fieldNames = td.returnFieldsArray();
		int index = 0;
		int i = td.numFields();
		while(i<td.numFields() + other.getDesc().numFields()){
			types[i] = other.getDesc().getType(index);
			fieldNames[i] = other.getDesc().getFieldName(index);
			index++;
			i++;
		}
		TupleDesc newTd = new TupleDesc(types, fieldNames);
		for(int j = 0; j < tuples.size(); j++){
			for(int k = 0; k < other.getTuples().size(); k++){
				if(tuples.get(j).getDesc().getType(field1) == Type.STRING){
						StringField fieldName1 = new StringField(tuples.get(j).getField(field1));
						StringField fieldName2 = new StringField(other.getTuples().get(k).getField(field2));
						if(fieldName1.compare(RelationalOperator.EQ, fieldName2)){
							Tuple tNew = new Tuple(newTd);
							tNew.setId(tuples.get(j).getId());
							tNew.setPid(tuples.get(j).getPid());
							for(int l = 0; l < tuples.get(j).getDesc().numFields(); l++){
								tNew.setField(l, tuples.get(j).getField(l));
							}
							int pointer = 0;
							int m = td.numFields();
							while(m<td.numFields() + other.getDesc().numFields()){
									tNew.setField(m, other.getTuples().get(k).getField(pointer));
									pointer++;
									m++;
							}
							resultTuples.add(tNew);
						}
					}
				else if(tuples.get(j).getDesc().getType(field1) == Type.INT){
					IntField fieldName1 = new IntField(tuples.get(j).getField(field1));
					IntField fieldName2 = new IntField(other.getTuples().get(k).getField(field2));
					if(fieldName1.compare(RelationalOperator.EQ, fieldName2)){
						Tuple tNew = new Tuple(newTd);
						tNew.setId(tuples.get(j).getId());
						tNew.setPid(tuples.get(j).getPid());
						for(int l = 0; l < tuples.get(j).getDesc().numFields(); l++){
							tNew.setField(l, tuples.get(j).getField(l));
						}
						int pointer = 0;
						int m = td.numFields();
						while(m<td.numFields() + other.getDesc().numFields() - 1){
								tNew.setField(m, other.getTuples().get(k).getField(pointer));
								pointer++;
								m++;
						}
						resultTuples.add(tNew);
					}
				}
				
//				if(tuples.get(j).getField(field1).equals(other.getTuples().get(k).getField(field2))){
//					System.out.println("first step");
//					Tuple tNew = new Tuple(newTd);
//					tNew.setId(tuples.get(j).getId());
//					tNew.setPid(tuples.get(j).getPid());
//					for(int l = 0; l < tuples.get(j).getDesc().numFields(); l++){
//						tNew.setField(l, tuples.get(j).getField(l));
//					}
//					int pointer = 0;
//					int m = td.numFields();
//					while(m<td.numFields() + other.getDesc().numFields() - 1){
//						if(!(pointer == field2)){
//							tNew.setField(m, other.getTuples().get(k).getField(pointer));
//
//							pointer++;
//							m++;
//						}
//						else{
//							pointer++;
//						}
//					}
//					System.out.println("added");
//					resultTuples.add(tNew);
//				}

			}

		}
		Relation result = new Relation(resultTuples, newTd);
		return result;
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
			Aggregator newAggregator = new Aggregator(op, groupBy, this.td);
			for(int i = 0; i < tuples.size(); i++){
				newAggregator.merge(tuples.get(i));
			}
			ArrayList<Tuple> aggResult = newAggregator.getResults();
			Relation result = new Relation(aggResult, this.td);
			return result;
		
	}
	
	public TupleDesc getDesc() {
		//your code here
		return td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		return null;
	}
}
