#include "planner/expression/bound_window_expression.hpp"

using namespace duckdb;
using namespace std;

BoundWindowExpression::BoundWindowExpression(ExpressionType type, TypeId return_type)
    : Expression(type, ExpressionClass::BOUND_WINDOW, return_type) {
}

string BoundWindowExpression::ToString() const {
	return "WINDOW";
}

bool BoundWindowExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundWindowExpression *)other_;

	if (start != other->start || end != other->end) {
		return false;
	}
	// check if the child expressions are equivalent
	if (!Expression::Equals(child.get(), other->child.get()) ||
	    !Expression::Equals(start_expr.get(), other->start_expr.get()) ||
	    !Expression::Equals(end_expr.get(), other->end_expr.get()) ||
	    !Expression::Equals(offset_expr.get(), other->offset_expr.get()) ||
	    !Expression::Equals(default_expr.get(), other->default_expr.get())) {
		return false;
	}

	// check if the partitions are equivalent
	if (partitions.size() != other->partitions.size()) {
		return false;
	}
	for (size_t i = 0; i < partitions.size(); i++) {
		if (!Expression::Equals(partitions[i].get(), other->partitions[i].get())) {
			return false;
		}
	}
	// check if the orderings are equivalent
	if (orders.size() != other->orders.size()) {
		return false;
	}
	for (size_t i = 0; i < orders.size(); i++) {
		if (orders[i].type != other->orders[i].type) {
			return false;
		}
		if (!BaseExpression::Equals((BaseExpression *)orders[i].expression.get(),
		                            (BaseExpression *)other->orders[i].expression.get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> BoundWindowExpression::Copy() {
	auto new_window = make_unique<BoundWindowExpression>(type, return_type);
	new_window->CopyProperties(*this);

	new_window->child = child ? child->Copy() : nullptr;
	for (auto &e : partitions) {
		new_window->partitions.push_back(e->Copy());
	}

	for (auto &o : orders) {
		BoundOrderByNode node;
		node.type = o.type;
		node.expression = o.expression->Copy();
		new_window->orders.push_back(move(node));
	}

	new_window->start = start;
	new_window->end = end;
	new_window->start_expr = start_expr ? start_expr->Copy() : nullptr;
	new_window->end_expr = end_expr ? end_expr->Copy() : nullptr;
	new_window->offset_expr = offset_expr ? offset_expr->Copy() : nullptr;
	new_window->default_expr = default_expr ? default_expr->Copy() : nullptr;

	return move(new_window);
}
