//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// parser/expression/columnref_expression.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

struct ColumnBinding {
	size_t table_index;
	size_t column_index;

	ColumnBinding() : table_index((size_t)-1), column_index((size_t)-1) {
	}
	ColumnBinding(size_t table, size_t column)
	    : table_index(table), column_index(column) {
	}

	bool operator==(const ColumnBinding &rhs) {
		return table_index == rhs.table_index &&
		       column_index == rhs.column_index;
	}
};

//! Represents a reference to a column from either the FROM clause or from an
//! alias
class ColumnRefExpression : public Expression {
  public:
	//! Only specify the column name, the table name will be derived later
	ColumnRefExpression(std::string column_name)
	    : Expression(ExpressionType::COLUMN_REF), reference(nullptr),
	      column_name(column_name) {
	}

	//! Specify both the column and table name
	ColumnRefExpression(std::string column_name, std::string table_name)
	    : Expression(ExpressionType::COLUMN_REF), reference(nullptr),
	      column_name(column_name), table_name(table_name) {
	}

	ColumnRefExpression(TypeId type, ColumnBinding binding)
	    : Expression(ExpressionType::COLUMN_REF, type), binding(binding),
	      reference(nullptr), column_name(""), table_name("") {
	}

	ColumnRefExpression(TypeId type, size_t index)
	    : Expression(ExpressionType::COLUMN_REF, type), index(index),
	      reference(nullptr), column_name(""), table_name("") {
	}

	const std::string &GetColumnName() const {
		return column_name;
	}
	const std::string &GetTableName() const {
		return table_name;
	}

	std::unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}

	std::string GetName() override {
		return !alias.empty() ? alias : column_name;
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::COLUMN_REF;
	}

	std::unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInfo *info, Deserializer &source);

	void ResolveType() override;

	bool Equals(const Expression *other) override;

	// FIXME: move these

	//! Column index set by the binder, used to access data in the executor
	ColumnBinding binding;

	//! Index used to access data in the chunks, set by the
	//! ColumnBindingResolver
	size_t index = (size_t)-1;

	//! Subquery recursion depth, needed for execution
	size_t depth = 0;

	//! A reference to the Expression this references, only used for
	//! alias references
	Expression *reference;
	//! Column name that is referenced
	std::string column_name;
	//! Table name of the column name that is referenced (optional)
	std::string table_name;

	std::string ToString() const override;

	bool IsScalar() override {
		return false;
	}
};
} // namespace duckdb
