//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/constraint/check_constraint.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/constraint.hpp"
#include "parser/expression.hpp"

namespace duckdb {

//! The CheckConstraint contains an expression that must evaluate to TRUE for
//! every row in a table
class CheckConstraint : public Constraint {
  public:
	CheckConstraint(std::unique_ptr<Expression> expression)
	    : Constraint(ConstraintType::CHECK), expression(move(expression)){};
	virtual ~CheckConstraint() {}

	virtual void Accept(SQLNodeVisitor *v) { v->Visit(*this); }

	virtual std::string ToString() const {
		return StringUtil::Format("CHECK(%s)", expression->ToString().c_str());
	}

	std::unique_ptr<Expression> expression;
};

} // namespace duckdb
