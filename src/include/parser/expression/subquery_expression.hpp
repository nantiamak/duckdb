//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/statement/select_statement.hpp"
#include "parser/tableref.hpp"

namespace duckdb {

//! Represents a subquery
class SubqueryExpression : public Expression {
public:
	SubqueryExpression() : Expression(ExpressionType::SELECT_SUBQUERY), subquery_type(SubqueryType::DEFAULT) {
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::SUBQUERY;
	}

	unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	bool Equals(const Expression *other) const override;

	unique_ptr<QueryNode> subquery;
	SubqueryType subquery_type;

	string ToString() const override {
		return "SUBQUERY";
	}

	bool HasSubquery() override {
		return true;
	}

	bool IsScalar() override {
		return false;
	}
};
} // namespace duckdb
