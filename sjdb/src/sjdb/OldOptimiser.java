package sjdb;

import java.util.*;

public class OldOptimiser {
    private Catalogue cat;
    private Estimator est;

    public OldOptimiser(Catalogue catalogue) {
        this.cat = catalogue;
        this.est = new Estimator();
    }

    public Operator optimise(Operator plan) {
        Operator pushedSel = pushSelects(plan);

        Operator reordered = reorder(pushedSel);

        Operator withJoins = createJoins(reordered);

        Operator pushedProj = pushProjects(withJoins);

        return pushedProj;
    }

    private Operator pushProjects(Operator plan) {
        if(noProjects(plan)) {
            return plan;
        } else {
            return pushProjects(plan, new HashSet<>());
        }
    }

    private Operator pushProjects(Operator plan, HashSet<Attribute> requiredAttrs) {
        Operator result = plan;

        if (plan instanceof Scan);
        else if (plan instanceof Select);
        else if (plan instanceof Project) {
            Project proj = (Project) plan;
            requiredAttrs.addAll(proj.getAttributes());
            result = new Project(pushProjects(proj.getInput(), requiredAttrs), proj.getAttributes());
        }
        else if (plan instanceof Join) {
            Join join = (Join) plan;
            Operator leftChild = join.getLeft();
            Operator rightChild = join.getRight();

            requiredAttrs.add(join.getPredicate().getLeftAttribute());
            requiredAttrs.add(join.getPredicate().getRightAttribute());

            List<Attribute> leftRequired = new ArrayList<>(leftChild.getOutput().getAttributes());
            if (leftRequired.retainAll(requiredAttrs)) {
                leftChild = new Project(leftChild, leftRequired);
            }

            List<Attribute> rightRequired = new ArrayList<>(rightChild.getOutput().getAttributes());
            if (rightRequired.retainAll(requiredAttrs)) {
                rightChild = new Project(rightChild, rightRequired);
            }

            result = new Join(
                    pushProjects(leftChild, requiredAttrs),
                    pushProjects(rightChild, requiredAttrs),
                    join.getPredicate()
            );
        }
        else if (plan instanceof Product) {
            Product prod = (Product) plan;
            result = new Product(
                    pushProjects(prod.getLeft(), requiredAttrs),
                    pushProjects(prod.getRight(), requiredAttrs)
            );
        }

        result.accept(est);
        return result;
    }

    private boolean noProjects(Operator plan) {
        if (plan instanceof Project) return false;
        else if (plan instanceof Scan) return true;
        else {
            for (Operator input : plan.getInputs()) {
                if (!noProjects(input)) return false;
            }
        }
        return true;
    }

    private Operator createJoins(Operator plan) {
        Operator result;
        if (canCreateJoin(plan)) {
            Select sel = (Select) plan;
            Product prod = (Product) sel.getInput();
            result = new Join(
                    createJoins(prod.getLeft()),
                    createJoins(prod.getRight()),
                    sel.getPredicate()
            );
        } else {
            result = newOpWithJoins(plan);
        }

        result.accept(est);
        return result;
    }

    private Operator newOpWithJoins(Operator op) {
        if (op instanceof Scan) return op;
        else if (op instanceof Select) {
            return new Select(createJoins(((Select) op).getInput()), ((Select) op).getPredicate());
        } else if (op instanceof Project) {
            return new Project(createJoins(((Project) op).getInput()), ((Project) op).getAttributes());
        } else if (op instanceof Product) {
            return new Product(createJoins(((Product) op).getLeft()), ((Product) op).getRight());
        } else if (op instanceof Join) {
            return new Join(createJoins(((Join) op).getLeft()), ((Join) op).getRight(), ((Join) op).getPredicate());
        } else return null;
    }

    private boolean canCreateJoin(Operator op) {
        if (op instanceof Select) {
            Select sel = ((Select) op);
            if (!sel.getPredicate().equalsValue()) {
                return sel.getInput() instanceof Product;
            }
        }
        return false;
    }

    private Operator reorder(Operator plan) {
        Map<Scan, Integer> relationSizes = orderScans(plan);

        List<Scan> orderedScans = new ArrayList<>();

        while (!relationSizes.isEmpty()) {
            Map.Entry<Scan, Integer> minValue = null;
            for (Map.Entry<Scan, Integer> entry : relationSizes.entrySet()) {
                    if(minValue == null || entry.getValue() < minValue.getValue()) {
                        minValue = entry;
                    }
            }
            orderedScans.add(minValue.getKey());
            relationSizes.remove(minValue.getKey());
        }

        return pushSelects(plan, orderedScans);
    }

    private Map<Scan, Integer> orderScans(Operator plan) {
        Map<Scan, Integer> relationSizes = new HashMap<>();

        if (plan instanceof Scan) {
            relationSizes.put((Scan) plan, plan.getOutput().getTupleCount());
        } else if (plan instanceof Select && ((Select) plan).getPredicate().equalsValue()) {
            relationSizes.put(findScans(plan).get(0), plan.getOutput().getTupleCount());
        } else {
            for (Operator input : plan.getInputs()) {
                relationSizes.putAll(orderScans(input));
            }
        }

        return relationSizes;
    }

    private Operator pushSelects(Operator plan, List<Scan> scanList) {
        Estimator est = new Estimator();
        List<Select> selects = findSelects(plan);
        List<Project> projects = findProjects(plan);

        Operator left = new Scan((NamedRelation) scanList.get(0).getRelation());
        Operator right;
        Operator acc;

        left = addPossibleSelects(selects, left);

        for (int i = 1; i < scanList.size(); i++) {

            right = new Scan((NamedRelation) scanList.get(i).getRelation());
            right = addPossibleSelects(selects, right);

            acc = new Product(left, right);
            acc.accept(est);
            left = acc;

            left = addPossibleSelects(selects, left);
        }


        for (Project project : projects) {
            left = new Project(left, project.getAttributes());
        }

        left.accept(est);
        return left;
    }

    private Operator pushSelects(Operator plan) {
        List<Scan> scans = findScans(plan);
        return pushSelects(plan, scans);
    }

    private Operator addPossibleSelects(List<Select> selects, Operator op) {
        List<Select> selectsToRemove = new ArrayList<>();
        for (Select select : selects) {
            if (containsAllAttrs(op, select)) {
                op = new Select(op, select.getPredicate());
                op.accept(est);
                selectsToRemove.add(select);
            }
        }

        selects.removeAll(selectsToRemove);
        selectsToRemove.clear();
        return op;
    }

    private boolean containsAllAttrs(Operator subTree, Select select) {
        boolean result = true;
        List<Attribute> attributeList = new ArrayList<>();
        Predicate predicate = select.getPredicate();

        attributeList.add(predicate.getLeftAttribute());
        if (!predicate.equalsValue()) {
            attributeList.add(predicate.getRightAttribute());
        }

        for (Attribute attribute : attributeList) {
            if (!subTree.getOutput().getAttributes().contains(attribute)) {
                result = false;
            }
        }

        return result;
    }

    private List<Select> findSelects(Operator plan) {
        List<Select> result = new ArrayList<>();

        if (plan instanceof Scan) {
            return result;
        } else {
            if (plan instanceof Select) {
                result.add((Select) plan);
            }
            for (Operator input : plan.getInputs()) {
                result.addAll(findSelects(input));
            }
            return result;
        }
    }

    private List<Scan> findScans(Operator plan) {
        List<Scan> result = new ArrayList<>();

        if (plan instanceof Scan) {
            result.add((Scan)plan);
            return result;
        } else {
            for (Operator input : plan.getInputs()) {
                result.addAll(findScans(input));
            }
            return result;
        }
    }

    private List<Project> findProjects(Operator plan) {
        List<Project> result = new ArrayList<>();

        if (plan instanceof Scan) {
            return result;
        } else {
            if (plan instanceof Project) {
                result.add((Project) plan);
            }
            for (Operator input : plan.getInputs()) {
                result.addAll(findProjects(input));
            }
            return result;
        }
    }
}