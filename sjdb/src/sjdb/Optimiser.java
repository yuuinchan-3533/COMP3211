package sjdb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class Optimiser {
	private List<Select> selectsToAdd = new ArrayList<>();
	private List<Project> projectsToAdd = new ArrayList<>();
	private List<Scan> scanToBeRebuiled = new ArrayList<>();
	private List<Scan> selectedScanToBeReOrdered = new ArrayList<>();
	private List<Scan> scanToBeReOrdered = new ArrayList<>();
	private HashMap<Scan, Scan> selectedScanToOriginalScan = new HashMap<>();
	private Catalogue cat;
	private Estimator est;

	public Optimiser(Catalogue catalogue) {
		this.cat = catalogue;
		this.est = new Estimator();
	}

	public Operator optimise(Operator originalRoot) {
		Operator movedSelectsDownRoot = moveSelectsDown(originalRoot);
		Operator reorderedJoinsRoot = reorderJoins(movedSelectsDownRoot);
		Operator createdJoinsRoot = createJoins(reorderedJoinsRoot);
		Operator movedProjectsDownRoot = moveProjectsDown(createdJoinsRoot);

		return movedProjectsDownRoot;
	}

	// add all select to list selectsToAdd
	private void generateSelectsToAdd(Operator node) {
		if (node instanceof Scan) {
			return;
		} else if (node instanceof Select) {
			selectsToAdd.add((Select) node);
		}
		for (Operator child : node.getInputs()) {
			generateSelectsToAdd(child);
		}
		return;
	}

	private void initiateProjectsToAdd(Operator node) {
		projectsToAdd = new ArrayList<>();
		generateProjectsToAdd(node);
	}

	// add all Project to list projectsToAdd
	private void generateProjectsToAdd(Operator node) {
		if (node instanceof Scan) {
			return;
		} else if (node instanceof Project) {
			projectsToAdd.add((Project) node);
		}
		for (Operator child : node.getInputs()) {
			generateProjectsToAdd(child);
		}
		return;
	}

	private void generateScanToBeRebuiled(Operator node) {
		if (node instanceof Scan) {
			scanToBeRebuiled.add((Scan) node);
			return;
		}
		for (Operator child : node.getInputs()) {
			generateScanToBeRebuiled(child);
		}
		return;
	}

	private Operator reorderJoins(Operator root) {
		generateSelectedScanToOriginalScan(root);
		generateSelectedScanToBeReOrdered();
		Operator reorderedJoinRes = moveSelectsDownByScanOrderList(root, scanToBeReOrdered);
		return reorderedJoinRes;
	}

	private void generateSelectedScanToOriginalScan(Operator root) {
		if (root instanceof Select && ((Select) root).getPredicate().equalsValue()) {
			// the select is attr = val

			Scan originalScan = (Scan) findSelectOriginalScan(root);
			// Relation selectedRelation = new Relation(root.getOutput().getTupleCount());

			NamedRelation selectedRelation = new NamedRelation(originalScan.toString(),
					root.getOutput().getTupleCount());
			List<Attribute> originalScanAttributes = originalScan.getRelation().getAttributes();

			for (Attribute attr : originalScanAttributes) {
				selectedRelation.addAttribute(attr);
			}

			Scan selectedScan = new Scan(selectedRelation);
			selectedScanToOriginalScan.put(selectedScan, originalScan);
			selectedScanToBeReOrdered.add(selectedScan);

		} else if (root instanceof Scan) {
			selectedScanToOriginalScan.put((Scan) root, (Scan) root);
			selectedScanToBeReOrdered.add((Scan) root);
		} else {
			for (Operator child : root.getInputs()) {
				generateSelectedScanToOriginalScan(child);
			}
		}
		return;
	}

	private void generateSelectedScanToBeReOrdered() {
		selectedScanToBeReOrdered.sort(new Comparator<Scan>() {
			@Override
			public int compare(Scan nodeA, Scan nodeB) {
				int diff = nodeA.getOutput().getTupleCount() - nodeB.getOutput().getTupleCount();
				return diff;
			}
		});
		for (Scan selectedScan : selectedScanToBeReOrdered) {
			scanToBeReOrdered.add(selectedScanToOriginalScan.get(selectedScan));
		}
	}

	private Operator findSelectOriginalScan(Operator node) {
		if (node instanceof Scan) {
			return node;
		}
		for (Operator child : node.getInputs()) {
			Operator res = findSelectOriginalScan(child);
			if (res != null) {
				return res;
			}
		}
		return null;
	}
	
	private List<Scan> findScans(Operator node) {
        List<Scan> scansRelatedToNode = new ArrayList<>();

        if (node instanceof Scan) {
        	scansRelatedToNode.add((Scan)node);
            return scansRelatedToNode;
        } else {
            for (Operator input : node.getInputs()) {
            	scansRelatedToNode.addAll(findScans(input));
            }
            return scansRelatedToNode;
        }
    }

	private Operator moveSelectsDown(Operator root) {
		generateScanToBeRebuiled(root);
		Operator movedSelectsDownRoot = moveSelectsDownByScanOrderList(root, scanToBeRebuiled);
		return movedSelectsDownRoot;
	}

	private Operator moveSelectsDownByScanOrderList(Operator root, List<Scan> scanOrderList) {
		generateSelectsToAdd(root);
		initiateProjectsToAdd(root);

		Estimator est = new Estimator();

		Operator leftChild = scanOrderList.get(0);

		for (int i = 1; i < scanOrderList.size(); i++) {
			Operator newLeftChild = addSelect(leftChild);
			Operator rightChild = scanOrderList.get(i);
			Operator newRightChild = addSelect(rightChild);

			Operator newNode = new Product(newLeftChild, newRightChild);
			newNode.accept(est);
			// newNode as leftChild
			leftChild = newNode;
		}
		leftChild = addSelect(leftChild);

		for (Project project : projectsToAdd) {
			Operator newLeftChild = new Project(leftChild, project.getAttributes());
			newLeftChild.accept(est);
			leftChild = newLeftChild;
		}

		return leftChild;
	}

	private Operator createJoins(Operator root) {
		Operator createdJoinsNode = createNewNodeInCreateJoins(root);
		return createdJoinsNode;
	}

	private boolean checkCanBeCombinedToJoin(Operator node) {
		// the Select node form is attr = attr and it's childNode is Product, it can
		// combined to Join
		if (node instanceof Select && (!((Select) node).getPredicate().equalsValue())) {
			Operator child = ((Select) node).getInput();
			if (child instanceof Product) {
				return true;
			}
		}
		return false;
	}

	private Operator moveProjectsDown(Operator root) {
		HashSet<Attribute> toBeProjectedAttrs = new HashSet<Attribute>();
		Operator res = createNewNodeInMoveProjectsDown(root, toBeProjectedAttrs);
		return res;
	}

	private Operator createNewNodeInMoveProjectsDown(Operator node, HashSet<Attribute> toBeProjectedAttrs) {
		Estimator est = new Estimator();

		if (node instanceof Project) {
			Operator child = ((Project) node).getInput();
			toBeProjectedAttrs.addAll(((Project) node).getAttributes());
			Operator newChild = createNewNodeInMoveProjectsDown(child, toBeProjectedAttrs);
			Operator newNode = new Project(newChild, ((Project) node).getAttributes());
			newNode.accept(est);
			return newNode;
		} else if (node instanceof Join) {
			Operator leftChild = ((Join) node).getLeft();
			Operator rightChild = ((Join) node).getRight();
			Predicate predicate = ((Join) node).getPredicate();
			List<Attribute> leftChildAttributes = leftChild.getOutput().getAttributes();
			List<Attribute> rightChildAttributes = rightChild.getOutput().getAttributes();
			toBeProjectedAttrs.add(predicate.getLeftAttribute());
			toBeProjectedAttrs.add(predicate.getRightAttribute());
			if (leftChildAttributes.retainAll(toBeProjectedAttrs)) {
				leftChild = new Project(leftChild, leftChildAttributes);
			}
			if (rightChildAttributes.retainAll(toBeProjectedAttrs)) {
				rightChild = new Project(rightChild, rightChildAttributes);
			}
			Operator newLeftChild = createNewNodeInMoveProjectsDown(leftChild, toBeProjectedAttrs);
			Operator newRightChild = createNewNodeInMoveProjectsDown(rightChild, toBeProjectedAttrs);

			Operator newNode = new Join(newLeftChild, newRightChild, predicate);
			newNode.accept(est);
			return newNode;
		} else if (node instanceof Select) {
			Operator child = ((Select) node).getInput();
			Predicate predicate = ((Select) node).getPredicate();
			Operator newChild = createNewNodeInMoveProjectsDown(child, toBeProjectedAttrs);
			Operator newNode = new Select(newChild, predicate);
			newNode.accept(est);
			return newNode;
		}
		return node;
	}

	private Operator createNewNodeInCreateJoins(Operator node) {
		Estimator est = new Estimator();

		if (checkCanBeCombinedToJoin(node)) {
			Operator child = ((Select) node).getInput();
			Operator leftOperator = ((Product) child).getLeft();
			Operator rightOperator = ((Product) child).getRight();
			Operator newLeftOperator = createNewNodeInCreateJoins(leftOperator);
			Operator newRightOperator = createNewNodeInCreateJoins(rightOperator);
			Join newNode = new Join(newLeftOperator, newRightOperator, ((Select) node).getPredicate());
			newNode.accept(est);
			return newNode;
		} else {
			if (node instanceof Scan) {
				return node;
			} else if (node instanceof Project) {
				Operator child = ((Project) node).getInput();
				Operator newChild = createNewNodeInCreateJoins(child);
				Project newNode = new Project(newChild, node.getOutput().getAttributes());
				newNode.accept(est);
				return newNode;
			} else if (node instanceof Product) {
				Operator leftChild = ((Product) node).getLeft();
				Operator rightChild = ((Product) node).getRight();
				Operator newLeftChild = createNewNodeInCreateJoins(leftChild);
				Operator newRightChild = createNewNodeInCreateJoins(rightChild);
				Product newNode = new Product(newLeftChild, newRightChild);
				newNode.accept(est);
				return newNode;
			} else if (node instanceof Join) {
				Operator leftChild = ((Join) node).getLeft();
				Operator rightChild = ((Join) node).getRight();
				Predicate predicate = ((Join) node).getPredicate();
				Operator newLeftChild = createNewNodeInCreateJoins(leftChild);
				Operator newRightChild = createNewNodeInCreateJoins(rightChild);
				Join newNode = new Join(newLeftChild, newRightChild, predicate);
				newNode.accept(est);
				return newNode;
			} else if (node instanceof Select) {
				Operator child = ((Select) node).getInput();
				Operator newChild = createNewNodeInCreateJoins(child);
				Predicate predicate = ((Select) node).getPredicate();
				Select newNode = new Select(newChild, predicate);
				newNode.accept(est);
				return newNode;
			}
		}
		return node;
	}

	private Operator addSelect(Operator child) {
		List<Select> selectsToRemove = new ArrayList<>();

		for (Select select : selectsToAdd) {
			if (checkSubtreeContainAllAttrs(child, select)) {
				// this node should add a select as its father
				
				Estimator est = new Estimator();
				Select tempChild = new Select(child, select.getPredicate());
				Predicate newPredicate = generateNewPredicate(tempChild);
				Select newChild = new Select(child,newPredicate);
				newChild.accept(est);
				child = newChild;
				selectsToRemove.add(select);
			}
		}

		selectsToAdd.removeAll(selectsToRemove);
		return child;
	}

	private boolean checkSubtreeContainAllAttrs(Operator subTree, Select select) {
		List<Attribute> subTreeAttributes = subTree.getOutput().getAttributes();
		Predicate predicate = select.getPredicate();
		// form attr = value
		if (predicate.equalsValue()) {
			Attribute target = select.getPredicate().getLeftAttribute();
			if (subTreeAttributes.contains(target)) {
				return true;
			} else {
				return false;
			}
		}
		// form attr = attr
		else {
			Attribute targetLeft = select.getPredicate().getLeftAttribute();
			Attribute targetRight = select.getPredicate().getRightAttribute();
			if (subTreeAttributes.contains(targetLeft) && subTreeAttributes.contains(targetRight)) {
				return true;
			} else {
				return false;
			}
		}
	}
	
	private Predicate generateNewPredicate(Select select) {
		Predicate predicate = select.getPredicate();
		if(predicate.equalsValue()) {
			return predicate;
		}
		// form attr = attr
		else {
			Attribute targetLeft = predicate.getLeftAttribute();
			Attribute targetRight = predicate.getRightAttribute();
			Operator child = select.getInput();
			if(child instanceof Product) {
				List<Scan> leftRelatedScans = findScans(((Product)child).getLeft());
				List<Scan> rightRelatedScans = findScans(((Product)child).getRight());
				HashSet<Attribute> leftScanAttrs = new HashSet<>();
				HashSet<Attribute> rightScanAttrs = new HashSet<>();
				for(Scan s : leftRelatedScans) {
					leftScanAttrs.addAll(s.getRelation().getAttributes());
				}
				for(Scan s : rightRelatedScans) {
					rightScanAttrs.addAll(s.getRelation().getAttributes());
				}
				
				if(rightScanAttrs.contains(targetLeft)&&leftScanAttrs.contains(targetRight)) {
					Predicate newPredicate = new Predicate(targetRight,targetLeft);
					return newPredicate;
				}
				
			}
			
			
		}
		return predicate;
	}
	

}
