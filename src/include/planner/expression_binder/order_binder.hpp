//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/order_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/unordered_map.hpp"
#include "parser/expression_map.hpp"
#include "parser/parsed_expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"

namespace duckdb {
class Expression;
class SelectNode;

//! The ORDER binder is responsible for binding an expression within the ORDER BY clause of a SQL statement
class OrderBinder {
public:
	OrderBinder(index_t projection_index, SelectNode &node, unordered_map<string, index_t> &alias_map,
	            expression_map_t<index_t> &projection_map, vector<unique_ptr<ParsedExpression>> &extra_select_list);

	unique_ptr<Expression> Bind(unique_ptr<ParsedExpression> expr);
	void RemapIndex(BoundColumnRefExpression &expr, index_t index);

private:
	unique_ptr<Expression> CreateProjectionReference(ParsedExpression &expr, index_t index);

	index_t projection_index;
	SelectNode &node;
	unordered_map<string, index_t> &alias_map;
	expression_map_t<index_t> &projection_map;
	vector<unique_ptr<ParsedExpression>> &extra_select_list;
};

} // namespace duckdb
