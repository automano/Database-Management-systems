package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	
	ArrayList<Tuple> tuples;
	private TupleDesc td;
	private AggregateOperator o;
	private boolean groupBy;
	ArrayList<Tuple> categories;

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.td = td;
		this.o = o;
		this.groupBy = groupBy;
		tuples = new ArrayList<Tuple>();
		categories = new ArrayList<Tuple>();
		
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		if (groupBy){
			tuples.add(t);
			boolean newCategory = true;
			if(t.getDesc().getType(0) == Type.INT){
				IntField tupleTypeInt = new IntField(t.getField(0));
				for (int i = 0; i < categories.size(); i++){
					IntField compareInt = new IntField(categories.get(i).getField(0));
					if(tupleTypeInt.compare(RelationalOperator.EQ, compareInt)){
						newCategory = false;
					}
				}
				if(newCategory){
					Tuple tCat = new Tuple(td);
					tCat.setField(0, tupleTypeInt.toByteArray());
					categories.add(tCat);
				}
			}
			if(t.getDesc().getType(0) == Type.STRING){
				StringField tupleTypeString = new StringField(t.getField(0));
				for (int i = 0; i < categories.size(); i++){
					StringField compareStr = new StringField(categories.get(i).getField(0));
					if(tupleTypeString.compare(RelationalOperator.EQ, compareStr)){
						newCategory = false;
					}
				}
				if(newCategory){
					Tuple tCat = new Tuple(td);
					tCat.setField(0, tupleTypeString.toByteArray());
					categories.add(tCat);
				}
				}
			}
		else{
		//your code here
		tuples.add(t);
		}
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		if(groupBy){
			switch (o) {
			case AVG:
				ArrayList<Tuple> newListAvg = new ArrayList<Tuple>();
				for(int i = 0; i < categories.size(); i++){
					int countA = 0;
					int totalA = 0;
					for(int j = 0; j < tuples.size(); j++){
						if(categories.get(i).getDesc().getType(0) == Type.INT){
							IntField categoryIntA = new IntField(categories.get(i).getField(0));
							IntField categoryCompareIntA = new IntField(tuples.get(i).getField(0));
							if(categoryIntA.compare(RelationalOperator.EQ, categoryCompareIntA)){
								IntField numberVal = new IntField(tuples.get(i).getField(1));
								totalA+= numberVal.getValue();
								countA++;
							}
						}
						if(categories.get(i).getDesc().getType(0) == Type.STRING){
							IntField categoryStrA = new IntField(categories.get(i).getField(0));
							IntField categoryCompareStrA = new IntField(tuples.get(i).getField(0));
							if(categoryStrA.compare(RelationalOperator.EQ, categoryCompareStrA)){
								IntField numberVal = new IntField(tuples.get(i).getField(1));
								totalA+= numberVal.getValue();
								countA++;
							}
						}
					}
					int averageA = totalA/countA;
					IntField averageInt = new IntField(averageA);
					Tuple tA = new Tuple(td);
					tA.setId(tuples.get(0).getId());
					tA.setPid(tuples.get(0).getPid());
					tA.setField(0, categories.get(i).getField(0));
					tA.setField(1, averageInt.toByteArray());
					newListAvg.add(tA);
				}
				tuples = newListAvg;
				break;
			case COUNT:
				ArrayList<Tuple> newListCount = new ArrayList<Tuple>();
				for(int i = 0; i < categories.size(); i++){
					int count = 0;
					for(int j = 0; j < tuples.size(); j++){
						if(categories.get(i).getDesc().getType(0) == Type.INT){
							IntField categoryInt = new IntField(categories.get(i).getField(0));
							IntField categoryCompareInt = new IntField(tuples.get(i).getField(0));
							if(categoryInt.compare(RelationalOperator.EQ, categoryCompareInt)){
								count++;
							}
						}
						if(categories.get(i).getDesc().getType(0) == Type.STRING){
							IntField categoryStr = new IntField(categories.get(i).getField(0));
							IntField categoryCompareStr = new IntField(tuples.get(i).getField(0));
							if(categoryStr.compare(RelationalOperator.EQ, categoryCompareStr)){
								count++;
							}
						}
					}
					IntField countInt = new IntField(count);
					Tuple tCount = new Tuple(td);
					tCount.setId(tuples.get(0).getId());
					tCount.setPid(tuples.get(0).getPid());
					tCount.setField(0, categories.get(i).getField(0));
					tCount.setField(1, countInt.toByteArray());
					newListCount.add(tCount);
				}
				tuples = newListCount;
				break;
			case MIN:
				ArrayList<Tuple> newListMi = new ArrayList<Tuple>();
				for(int i = 0; i < categories.size(); i++){
					IntField minTuple = new IntField(10000);
					StringField minString = new StringField("hi");
					boolean isInt = false;
					for(int j = 0; j < tuples.size(); j++){
						boolean first = true;
						if(categories.get(i).getDesc().getType(0) == Type.INT){
							isInt = true;
							IntField categoryIntA = new IntField(categories.get(i).getField(0));
							IntField categoryCompareIntA = new IntField(tuples.get(j).getField(0));
							if(categoryIntA.compare(RelationalOperator.EQ, categoryCompareIntA)){
								if(first){
									IntField replaceTuple = new IntField(tuples.get(j).getField(1));
									minTuple = replaceTuple;
									first = false;
								}
								IntField replaceTuple2 = new IntField(tuples.get(j).getField(1));
								if(minTuple.compare(RelationalOperator.GTE, replaceTuple2)){
									minTuple = replaceTuple2;
								}
							}
						}
						if(categories.get(i).getDesc().getType(0) == Type.STRING){
							IntField categoryStrA = new IntField(categories.get(i).getField(0));
							IntField categoryCompareStrA = new IntField(tuples.get(i).getField(0));
							if(categoryStrA.compare(RelationalOperator.EQ, categoryCompareStrA)){
								if(first){
									StringField replaceString = new StringField(tuples.get(j).getField(1));
									minString = replaceString;
									first = false;
								}
								StringField replaceString2 = new StringField(tuples.get(j).getField(1));
								if(minString.compare(RelationalOperator.GTE, replaceString2)){
									minString = replaceString2;
								}
							}
						}
					}
					Tuple tMi = new Tuple(td);
					tMi.setId(tuples.get(0).getId());
					tMi.setPid(tuples.get(0).getPid());
					tMi.setField(0, categories.get(i).getField(0));
					if(isInt){
						tMi.setField(1, minTuple.toByteArray());
					}
					else{
						tMi.setField(1, minString.toByteArray());
					}
					newListMi.add(tMi);
				}
				tuples = newListMi;
				break;
			case MAX:
				ArrayList<Tuple> newListMa = new ArrayList<Tuple>();
				for(int i = 0; i < categories.size(); i++){
					IntField maxTuple = new IntField(10000);
					StringField maxString = new StringField("hi");
					boolean isInt2 = false;
					for(int j = 0; j < tuples.size(); j++){
						boolean first2 = true;
						if(categories.get(i).getDesc().getType(0) == Type.INT){
							isInt2 = true;
							IntField categoryIntA2 = new IntField(categories.get(i).getField(0));
							IntField categoryCompareIntA2 = new IntField(tuples.get(j).getField(0));
							if(categoryIntA2.compare(RelationalOperator.EQ, categoryCompareIntA2)){
								if(first2){
									IntField replaceTupleMa = new IntField(tuples.get(j).getField(1));
									maxTuple = replaceTupleMa;
									first2 = false;
								}
								IntField replaceTupleMa2 = new IntField(tuples.get(j).getField(1));
								if(maxTuple.compare(RelationalOperator.LTE, replaceTupleMa2)){
									maxTuple = replaceTupleMa2;
								}
							}
						}
						if(categories.get(i).getDesc().getType(0) == Type.STRING){
							IntField categoryStrA2 = new IntField(categories.get(i).getField(0));
							IntField categoryCompareStrA2 = new IntField(tuples.get(i).getField(0));
							if(categoryStrA2.compare(RelationalOperator.EQ, categoryCompareStrA2)){
								if(first2){
									StringField replaceStringMa = new StringField(tuples.get(j).getField(1));
									maxString = replaceStringMa;
									first2 = false;
								}
								StringField replaceStringMa2 = new StringField(tuples.get(j).getField(1));
								if(maxString.compare(RelationalOperator.LTE, replaceStringMa2)){
									maxString = replaceStringMa2;
								}
							}
						}
					}
					Tuple tMa = new Tuple(td);
					tMa.setId(tuples.get(0).getId());
					tMa.setPid(tuples.get(0).getPid());
					tMa.setField(0, categories.get(i).getField(0));
					if(isInt2){
						tMa.setField(1, maxTuple.toByteArray());
					}
					else{
						tMa.setField(1, maxString.toByteArray());
					}
					newListMa.add(tMa);
				}
				tuples = newListMa;
				break;
			case SUM:
				ArrayList<Tuple> newListSum = new ArrayList<Tuple>();
				for(int i = 0; i < categories.size(); i++){
					int totalS = 0;
					for(int j = 0; j < tuples.size(); j++){
						if(categories.get(i).getDesc().getType(0) == Type.INT){
							IntField categoryIntA = new IntField(categories.get(i).getField(0));
							IntField categoryCompareIntA = new IntField(tuples.get(i).getField(0));
							if(categoryIntA.compare(RelationalOperator.EQ, categoryCompareIntA)){
								IntField numberVal = new IntField(tuples.get(i).getField(1));
								totalS+= numberVal.getValue();
							}
						}
						if(categories.get(i).getDesc().getType(0) == Type.STRING){
							IntField categoryStrA = new IntField(categories.get(i).getField(0));
							IntField categoryCompareStrA = new IntField(tuples.get(i).getField(0));
							if(categoryStrA.compare(RelationalOperator.EQ, categoryCompareStrA)){
								IntField numberVal = new IntField(tuples.get(i).getField(1));
								totalS+= numberVal.getValue();
							}
						}
					}
					IntField sumInt = new IntField(totalS);
					Tuple tS = new Tuple(td);
					tS.setId(tuples.get(0).getId());
					tS.setPid(tuples.get(0).getPid());
					tS.setField(0, categories.get(i).getField(0));
					tS.setField(1, sumInt.toByteArray());
					newListSum.add(tS);
				}
				
				tuples = newListSum;
				break;
			}
			return tuples;
		}
		else{
			switch (o) {
			case AVG:
				int countAvg = 0;
				int totalAvg = 0;
				int idAvg = 0;
				int pidAvg = 0;
				for (int i = 0; i < tuples.size(); i++){
					countAvg++;
					IntField fieldName1 = new IntField(tuples.get(i).getField(0));
					totalAvg += fieldName1.getValue();
					idAvg = tuples.get(i).getId();
					pidAvg = tuples.get(i).getPid();
				}
				int average = totalAvg/countAvg;
				Tuple tAvg = new Tuple(td);
				tAvg.setId(idAvg);
				tAvg.setPid(pidAvg);
				IntField averageInt = new IntField(average);
				tAvg.setField(0, averageInt.toByteArray());
				ArrayList<Tuple> newListAvg = new ArrayList<Tuple>();
				newListAvg.add(tAvg);
				tuples = newListAvg;
				break;
			case COUNT:
				int count = 0;
				int id = 0;
				int pid = 0;
				for (int i = 0; i < tuples.size(); i++){
					count++;
					id = tuples.get(i).getId();
					pid = tuples.get(i).getPid();
				}
				Tuple t = new Tuple(td);
				t.setId(id);
				t.setPid(pid);
				IntField totalCounter = new IntField(count);
				t.setField(0, totalCounter.toByteArray());
				ArrayList<Tuple> newListCount = new ArrayList<Tuple>();
				newListCount.add(t);
				tuples = newListCount;
				break;
			case MIN:
				int idMin = 0;
				int pidMin = 0;
				boolean wasInt = false;
				IntField fieldName1min = new IntField(10000);
				StringField fieldName1MinStr = new StringField("hi");
				for (int i = 0; i < tuples.size(); i++){
					idMin = tuples.get(i).getId();
					pidMin = tuples.get(i).getPid();
					if(tuples.get(i).getDesc().getType(0) == Type.INT){
						wasInt = true;
						if(i == 0){
							IntField replace = new IntField(tuples.get(i).getField(0));
							fieldName1min = replace;
						}
						IntField fieldName2min = new IntField(tuples.get(i).getField(0));
						if(fieldName1min.compare(RelationalOperator.GTE, fieldName2min)){
							fieldName1min = fieldName2min;
						}
					}
					else if(tuples.get(i).getDesc().getType(0) == Type.STRING){
						if(i == 0){
							StringField replaceStr = new StringField(tuples.get(i).getField(0));
							fieldName1MinStr = replaceStr;
						}
						StringField fieldName2MinStr = new StringField(tuples.get(i).getField(0));
						if(fieldName1MinStr.compare(RelationalOperator.GTE, fieldName2MinStr)){
							fieldName1MinStr = fieldName2MinStr;
						}
					}
				}
				Tuple tMin = new Tuple(td);
				tMin.setId(idMin);
				tMin.setPid(pidMin);
				if(wasInt){
					tMin.setField(0, fieldName1min.toByteArray());
				}
				else{
					tMin.setField(0, fieldName1MinStr.toByteArray());
				}
				ArrayList<Tuple> newListMin = new ArrayList<Tuple>();
				newListMin.add(tMin);
				tuples = newListMin;
				break;
			case MAX:
				int idMax = 0;
				int pidMax = 0;
				boolean wasIntMax = false;
				IntField fieldName1max = new IntField(10000);
				StringField fieldName1MaxStr = new StringField("hi");
				for (int i = 0; i < tuples.size(); i++){
					idMax = tuples.get(i).getId();
					pidMax = tuples.get(i).getPid();
					if(tuples.get(i).getDesc().getType(0) == Type.INT){
						wasIntMax = true;
						if(i == 0){
							IntField replaceMax = new IntField(tuples.get(i).getField(0));
							fieldName1max = replaceMax;
						}
						IntField fieldName2max = new IntField(tuples.get(i).getField(0));
						if(fieldName1max.compare(RelationalOperator.LTE, fieldName2max)){
							fieldName1max = fieldName2max;
						}
					}
					else if(tuples.get(i).getDesc().getType(0) == Type.STRING){
						if(i == 0){
							StringField replaceStrMax = new StringField(tuples.get(i).getField(0));
							fieldName1MaxStr = replaceStrMax;
						}
						StringField fieldName2MaxStr = new StringField(tuples.get(i).getField(0));
						if(fieldName1MaxStr.compare(RelationalOperator.LTE, fieldName2MaxStr)){
							fieldName1MaxStr = fieldName2MaxStr;
						}
					}
				}
				Tuple tMax = new Tuple(td);
				tMax.setId(idMax);
				tMax.setPid(pidMax);
				if(wasIntMax){
					tMax.setField(0, fieldName1max.toByteArray());
				}
				else{
					tMax.setField(0, fieldName1MaxStr.toByteArray());
				}
				ArrayList<Tuple> newListMax = new ArrayList<Tuple>();
				newListMax.add(tMax);
				tuples = newListMax;
				break;
			case SUM:
				int totalSum = 0;
				int idSum = 0;
				int pidSum = 0;
				for (int i = 0; i < tuples.size(); i++){
					IntField fieldName1Sum = new IntField(tuples.get(i).getField(0));
					totalSum += fieldName1Sum.getValue();
					idSum = tuples.get(i).getId();
					pidSum = tuples.get(i).getPid();
				}
				IntField totalSumMaker = new IntField(totalSum);
				Tuple tSum = new Tuple(td);
				tSum.setId(idSum);
				tSum.setPid(pidSum);
				tSum.setField(0, totalSumMaker.toByteArray());
				ArrayList<Tuple> newListSum = new ArrayList<Tuple>();
				newListSum.add(tSum);
				tuples = newListSum;
				break;
			default:
				throw new UnsupportedOperationException("Aggregate Functions only");

			}
			return tuples;
		}
	}

}
