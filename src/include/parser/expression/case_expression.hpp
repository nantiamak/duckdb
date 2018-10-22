//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/case_expression.hpp
//
// Author: Hannes Mühleisen
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {

class CaseExpression : public Expression {
  public:
	// this expression has 3 children, the test and the result if the test
	// evaluates to 1 and the result if it does not
	CaseExpression() : Expression(ExpressionType::OPERATOR_CASE_EXPR) {
	}

	virtual void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::CASE;
	}

	virtual std::unique_ptr<Expression> Copy() override;

	//! Deserializes a blob back into an CaseExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);

	virtual void ResolveType() override;
};
} // namespace duckdb
