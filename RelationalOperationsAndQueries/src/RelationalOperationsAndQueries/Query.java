package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.AddAliasesVisitor;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;

		// sb = root of the tree
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		
		//your code here
				
		System.out.println("Statement " +statement);

		// Tuples 
		Catalog c = Database.getCatalog();
		// from table
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> tableNamesList = tablesNamesFinder.getTableList(selectStatement);
		for(String t: tableNamesList) {
			
			System.out.println("tableNames: " +t);
		}

		String table1name = tableNamesList.get(0);
		if(tableNamesList.size() == 2) {
			String table2name = tableNamesList.get(1);
			int table2id = c.getTableId(table2name);
			TupleDesc td2 = c.getTupleDesc(table2id);
			
			ArrayList<Tuple> allTuples2 = c.getDbFile(c.getTableId(table1name)).getAllTuples();
			Relation relation2 = new Relation(allTuples2, td2);

		}
		int table1id = c.getTableId(table1name);
		TupleDesc td1 = c.getTupleDesc(table1id);
		
		ArrayList<Tuple> allTuples1 = c.getDbFile(c.getTableId(table1name)).getAllTuples();
		Relation relation1 = new Relation(allTuples1, td1);


		
		// order of execution - from, where, select, group by, order by

		ColumnVisitor cv = new ColumnVisitor();
		List<SelectItem> selectItemsList = sb.getSelectItems();
		ArrayList<Integer> fieldsNum = new ArrayList<Integer>();

		System.out.println("selectItemsList size: " +selectItemsList.size());

		if(selectItemsList.contains("*")) {
			return relation1;
		}
		
		else {
			for(int i = 0; i < selectItemsList.size(); i++) {
				System.out.println("i = " +i);
				selectItemsList.get(i).accept(cv);


				System.out.println("selectItemsList.get(i):" +selectItemsList.get(i));
				//			System.out.println("cvgetcolumn:" +cv.getColumn());	
				fieldsNum.add(td1.nameToId(cv.getColumn()));
			}
			// ------------- select * ---------------
			//			if(selectItemsList.get(i).equals("*")) {
			//				System.out.println("whole relation");
			//				return relation1;
			//				selectItemsList.remove('*');
			////				selectItemsList.add(td1.returnFieldsArray()[i]);
			//				// add all columns
			////				selectItemsList.addAll(c.getDbFile(table1id).getFile());
			//			}

		}

		System.out.println();
//			System.out.println("selectItems: " +selectItemsList.get(i));
//			System.out.println(i + ") cv: " +cv.getColumn());		
//		System.out.println("cv.getcolumn: " +cv.getColumn());
		
		

		
		// select condition (where in SQL)
		WhereExpressionVisitor whereExpression = new WhereExpressionVisitor();

		Relation relationAfterWhere = relation1;
		Relation relationAfterJoin = relationAfterWhere;
		Relation relationAfterAggregate = relationAfterJoin;
		Relation relationAfterProject = relation1.project(fieldsNum);

		
		if(sb.getWhere() != null) {

			sb.getWhere().accept(whereExpression);
			RelationalOperator relOp = whereExpression.getOp();
			
			List<String> whereExpressionsList = new ArrayList<String>();

			for(int i = 0; i < 2; i++) {
				whereExpressionsList.add("getWhere(): " +sb.getWhere());
			}
//			System.out.println("nameToId: " +td1.nameToId(whereExpression.getLeft()));
			int tempField = td1.nameToId(whereExpression.getLeft());
			relationAfterWhere = relation1.select(td1.nameToId(whereExpression.getLeft()), whereExpression.getOp(), whereExpression.getRight()); 
			
			// join
			if(sb.getJoins() != null) {

				List<Join> joins = sb.getJoins();
				WhereExpressionVisitor joinCondition;

				FromItem joinLeftItem = sb.getFromItem();
				FromItem joinRightItem = joins.get(0).getRightItem();

				Expression onExpression = joins.get(0).getOnExpression();
				joinCondition = (WhereExpressionVisitor) onExpression;
				String leftCondition = joinCondition.getLeft();
				String rightCondition = joinCondition.getLeft();

				int field1 = td1.nameToId(leftCondition);
				int field2 = td1.nameToId(rightCondition);

				relationAfterJoin = relationAfterWhere.join((Relation)joinRightItem,field1, field2);

				System.out.println("joins size: " +joins.size());
				System.out.println("join on: " +joins.get(0).getOnExpression());
				System.out.println("joins.get(0).getUsingColumns(): " +joins.get(0).getUsingColumns());
				relationAfterProject = relationAfterJoin.project(fieldsNum);
				
				ColumnVisitor checkGroupBy = new ColumnVisitor();
				
				for(int i = 0; i < selectItemsList.size(); i++) {
					selectItemsList.get(i).accept(checkGroupBy);
					if(checkGroupBy.isAggregate()) {
						boolean isGroupBy = (sb.getGroupByColumnReferences() != null);
						relationAfterAggregate = relationAfterJoin.aggregate(checkGroupBy.getOp(), isGroupBy);
					}
				}
			}
			relationAfterProject = relationAfterWhere.project(fieldsNum);
		}	

		Relation relationAfterRename = relationAfterProject;
		
		for(int i = 0; i < selectItemsList.size(); i++) {
			if(sb.getFromItem().getAlias() != null) {
				ArrayList<String> columnsToRename = new ArrayList<String>();
				ArrayList<Integer> columnNum = new ArrayList<Integer>();

				ArrayList<String> renameToList = new ArrayList<String>();
				renameToList.add(sb.getFromItem().getAlias().getName());

				selectItemsList.get(i).accept(cv);
				columnsToRename.add(cv.getColumn());
				columnNum.add(td1.nameToId(columnsToRename.get(i)));

				relationAfterRename = relationAfterProject.rename(columnNum, renameToList);

				System.out.println("getAlias: " +sb.getFromItem().getAlias());
			}
		}

		return relationAfterRename;
		
//		System.out.println("getWhere: " +sb.getWhere().toString());
//		System.out.println("getleft:" +whereExpression.getLeft()+ 
//				" field:" +whereExpression.getRight()+ 
//				" op: " +whereExpression.getOp());
		
				


//		return relationAfterWhere;

//		List<Expression> selectExpressionItem = statement.getExpressions();
		
//		return relationAfterProject;
//		
//		WhereExpressionVisitor wev = new WhereExpressionVisitor();
////		selectExpressionItem.accept(wev);
////	
//		System.out.println("cv.getcolumn: " +cv.getColumn());
//
//		
		
//		return relationAfterProject;

//			for(int i = 0; i < 2; i++) {
//				whereExpressionsList.add("getWhere(): " +sb.getWhere());
//			}
////			System.out.println("nameToId: " +td1.nameToId(whereExpression.getLeft()));
//			int tempField = td1.nameToId(whereExpression.getLeft());
//			relationAfterWhere = relation1.select(td1.nameToId(whereExpression.getLeft()), whereExpression.getOp(), whereExpression.getRight()); 
//			relationAfterProject = relationAfterWhere.project(fieldsNum);			
//
//		}
//		else {
//			relationAfterProject = relation1.project(fieldsNum);
//
//		}

		// aggregate
//		AggregateExpressionVisitor aev = new AggregateExpressionVisitor();
//		node.accept(aev);
		
//		System.out.println("cv.getColumn: " +cv.getColumn()+
//				"\naev.getOp: " +cv.getOp());
		
//		if(cv.isAggregate()) {
//			Relation relationAfterJoin;
//
//			List<Join> joins = sb.getJoins();
//
//			FromItem joinLeftItem = sb.getFromItem();
//			FromItem joinRightItem = joins.get(0).getRightItem();
//			
//			Expression onExpression = joins.get(0).getOnExpression();
//			
//			System.out.println("joins size: " +joins.size());
//			System.out.println("join on: " +joins.get(0).getOnExpression());
//		}
//		return relationAfterProject;
		
		
////		// group by
////		System.out.println("Group by: " +sb.getGroupByColumnReferences());
////		
//////		return result.select(sb.getSelectItems(), whereExpression.getOp(), whereExpression.getRight());
////		return relation.select(whereExpression.getLeft(), whereExpression.getOp(), whereExpression.getRight());
//		
//		
//		
//		// alias
//		AddAliasesVisitor aav = new AddAliasesVisitor();
//		sb.accept(aav);
//		System.out.println("sb.getFromItem(): " +sb.getFromItem());
//		
//		
//		System.out.println("-------------------------");
//		
//		
//		
		//		return null;
		//		
	}
}
