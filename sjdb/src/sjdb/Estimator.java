package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
		// get the attributes to be projected
		List<Attribute> toBeProjectedAttributes = op.getAttributes();
		// get the original table
		Relation originalTable=op.getInput().getOutput();
		// the res tuple count is equal to original table's tuple count
		Relation res = new Relation(originalTable.getTupleCount());
		// the res attributes is equal to the attributes to be projected
		for(Attribute a : toBeProjectedAttributes) {
			res.addAttribute(new Attribute(a));
		}
		op.setOutput(res);
		
	}
	
	public void visit(Select op) {
		Relation originalTable=op.getInput().getOutput();
		Predicate p = op.getPredicate();
		Attribute toBeSelectedAttribute = p.getLeftAttribute();
		int resTupleCnt=0;
		List<Attribute> resAttributes = new ArrayList<Attribute>();
		Relation res = null;
		// the predicates form is attr = val
		if(p.equalsValue()) {
			
			resTupleCnt=originalTable.getTupleCount()/(originalTable.getAttribute(toBeSelectedAttribute).getValueCount());
			res = new Relation(resTupleCnt);
			List<Attribute> originalTableAttributes=originalTable.getAttributes();
			for(Attribute a : originalTableAttributes) {
				if(a.equals(toBeSelectedAttribute)) {
					res.addAttribute(new Attribute(a.getName(),1));
				}else {
					res.addAttribute(new Attribute(a));
				}
			}
		}else{
			// the predicates form is attr = attr
			Attribute leftAttribute = originalTable.getAttribute(p.getLeftAttribute());
			Attribute rightAttribute = originalTable.getAttribute(p.getRightAttribute());
			int leftValueCount = leftAttribute.getValueCount();
			int rightValueCount = rightAttribute.getValueCount();
			
			resTupleCnt = originalTable.getTupleCount()/Math.max(leftValueCount, rightValueCount);
			res = new Relation(resTupleCnt);
			
			List<Attribute> originalTableAttributes=originalTable.getAttributes();
			for(Attribute a : originalTableAttributes) {
				if(a.equals(toBeSelectedAttribute)) {
					res.addAttribute(new Attribute(a.getName(),Math.min(leftValueCount, rightValueCount)));
				}else {
					res.addAttribute(new Attribute(a));
				}
			}
		}
		op.setOutput(res);
		
		
	}

	
	public void visit(Product op) {
		// get left_table
		Relation left_table=op.getLeft().getOutput();
		// get right table
		Relation right_table=op.getRight().getOutput();
		// the res_tuple_count = left_tuple_cout * right_tuple_cout
		Relation res = new Relation(left_table.getTupleCount()*right_table.getTupleCount());
		
		// get the left_table attributes
		List<Attribute> left_table_attributes = left_table.getAttributes();
		
		// get the right_table attributes
		List<Attribute> right_table_attributes = right_table.getAttributes();
		// the res attributes equals left_table attributes + right_table_attributes
		for(Attribute left_a : left_table_attributes) {
			res.addAttribute(new Attribute(left_a));
		}
		for(Attribute right_a : right_table_attributes){
			res.addAttribute(new Attribute(right_a));
		}
		op.setOutput(res);
		
	}
	
	public void visit(Join op) {
		Relation leftTable=op.getLeft().getOutput();
		Relation rightTable=op.getRight().getOutput();
		Attribute leftJoinAttribute = op.getPredicate().getLeftAttribute();
		Attribute rightJoinAttribute = op.getPredicate().getRightAttribute();
		int vLA=leftTable.getAttribute(leftJoinAttribute).getValueCount();
		int vRA=rightTable.getAttribute(rightJoinAttribute).getValueCount();
		
		int resTupleCnt=leftTable.getTupleCount()*rightTable.getTupleCount()/Math.max(vLA, vRA);
		Relation res=new Relation(resTupleCnt);
		List<Attribute> leftTableAttributes=leftTable.getAttributes();
		for(Attribute lA : leftTableAttributes) {
			if(lA.equals(leftJoinAttribute)) {
				res.addAttribute(new Attribute(lA.getName(),Math.min(vLA, vRA)));
			}else {
				res.addAttribute(new Attribute(lA));
			}
		}
		
		List<Attribute> rightTableAttributes=rightTable.getAttributes();
		for(Attribute rA : rightTableAttributes) {
			if(rA.equals(rightJoinAttribute)) {
				res.addAttribute(new Attribute(rA.getName(),Math.min(vLA, vRA)));
			}else {
				res.addAttribute(new Attribute(rA));
			}
		}
		op.setOutput(res);
	}

}
