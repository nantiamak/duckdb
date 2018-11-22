//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/aggregate_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
//! The AggregateExpression represents an aggregate in the query
class AggregateExpression : public Expression {
  public:
	AggregateExpression(ExpressionType type, std::unique_ptr<Expression> child);

	//! Resolve the type of the aggregate
	virtual void ResolveType() override;

	virtual void
	GetAggregates(std::vector<AggregateExpression *> &expressions) override;
	virtual bool IsAggregate() override {
		return true;
	}

	virtual std::unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::AGGREGATE;
	}

	virtual std::unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);

	virtual std::string GetName() override;

	size_t index;

  private:
};
} // namespace duckdb
