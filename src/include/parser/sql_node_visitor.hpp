//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/sql_node_visitor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/constraint.hpp"
#include "parser/parsed_expression.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"
#include "parser/tokens.hpp"

namespace duckdb {

//! The SQLNodeVisitor is an abstract base class that implements the Visitor
//! pattern on Expression and SQLStatement. It will visit nodes
//! recursively and call the Visit expression corresponding to the expression
//! visited.
class SQLNodeVisitor {
public:
	virtual ~SQLNodeVisitor(){};

	//! Visits a generic expression and calls the specialized Visit method for the expression type, then visits its
	//! children recursively using the VisitExpressionChildren method. Be careful when calling this method as it will
	//! not call the VisitReplace method.
	virtual void VisitExpression(ParsedExpression *expr_ptr);
	//! Visits a generic expression and calls the specialized VisitReplace and Visit methods for the expression type,
	//! then visits its children recursively using the VisitExpressionChildren method
	virtual void VisitExpression(unique_ptr<ParsedExpression> *expression);

protected:
	// The VisitExpressionChildren method is called at the end of every call to VisitExpression to recursively visit all
	// expressions in an expression tree. It can be overloaded to prevent automatically visiting the entire tree.
	virtual void VisitExpressionChildren(ParsedExpression &expression);

	// The Visit methods can be overloaded if the inheritee of this class wishes to only Visit expressions without
	// replacing them
	virtual void Visit(AggregateExpression &expr) {
	}
	virtual void Visit(CaseExpression &expr) {
	}
	virtual void Visit(CastExpression &expr) {
	}
	virtual void Visit(ColumnRefExpression &expr) {
	}
	virtual void Visit(ComparisonExpression &expr) {
	}
	virtual void Visit(ConjunctionExpression &expr) {
	}
	virtual void Visit(ConstantExpression &expr) {
	}
	virtual void Visit(DefaultExpression &expr) {
	}
	virtual void Visit(FunctionExpression &expr) {
	}
	virtual void Visit(OperatorExpression &expr) {
	}
	virtual void Visit(ParameterExpression &expr) {
	}
	virtual void Visit(StarExpression &expr) {
	}
	virtual void Visit(SubqueryExpression &expr) {
	}
	virtual void Visit(WindowExpression &expr) {
	}

	// The VisitReplace method can be overloaded if the inheritee of this class wishes to replace expressions while
	// visiting them
	virtual unique_ptr<ParsedExpression> VisitReplace(AggregateExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(CaseExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(CastExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(ColumnRefExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(ComparisonExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(ConjunctionExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(ConstantExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(DefaultExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(FunctionExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(OperatorExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(ParameterExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(StarExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(SubqueryExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);
	virtual unique_ptr<ParsedExpression> VisitReplace(WindowExpression &expr, unique_ptr<ParsedExpression> *expr_ptr);

public:
	virtual void Visit(NotNullConstraint &expr) {
	}
	virtual void Visit(CheckConstraint &expr);
	virtual void Visit(ParsedConstraint &expr) {
	}

	virtual unique_ptr<TableRef> Visit(BaseTableRef &expr) {
		return nullptr;
	};
	virtual unique_ptr<TableRef> Visit(CrossProductRef &expr);
	virtual unique_ptr<TableRef> Visit(JoinRef &expr);
	virtual unique_ptr<TableRef> Visit(SubqueryRef &expr) {
		return nullptr;
	};
	virtual unique_ptr<TableRef> Visit(TableFunction &expr) {
		return nullptr;
	};

	template <class T> void AcceptChild(unique_ptr<T> *accept) {
		assert(accept);
		auto accept_res = (*accept)->Accept(this);
		if (accept_res) {
			(*accept) = unique_ptr<T>((T *)accept_res.release());
		}
	}
};
} // namespace duckdb
