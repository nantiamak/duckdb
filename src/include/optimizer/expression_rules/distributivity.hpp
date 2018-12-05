//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/expression_rules/distributivity.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

// (X AND B) OR (X AND C) OR (X AND D) = X AND (B OR C OR D)
class DistributivityRule : public Rule {
public:
	DistributivityRule();

	unique_ptr<Expression> Apply(Rewriter &rewriter, Expression &root, vector<AbstractOperator> &bindings,
	                             bool &fixed_point);
};

} // namespace duckdb
