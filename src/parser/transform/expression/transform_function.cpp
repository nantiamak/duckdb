#include "parser/expression/cast_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/expression/star_expression.hpp"
#include "parser/expression/window_expression.hpp"
#include "parser/transformer.hpp"
#include "common/string_util.hpp"
#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

static ExpressionType WindowToExpressionType(string &fun_name) {
	if (fun_name == "rank") {
		return ExpressionType::WINDOW_RANK;
	} else if (fun_name == "rank_dense" || fun_name == "dense_rank") {
		return ExpressionType::WINDOW_RANK_DENSE;
	} else if (fun_name == "percent_rank") {
		return ExpressionType::WINDOW_PERCENT_RANK;
	} else if (fun_name == "row_number") {
		return ExpressionType::WINDOW_ROW_NUMBER;
	} else if (fun_name == "first_value" || fun_name == "first") {
		return ExpressionType::WINDOW_FIRST_VALUE;
	} else if (fun_name == "last_value" || fun_name == "last") {
		return ExpressionType::WINDOW_LAST_VALUE;
	} else if (fun_name == "cume_dist") {
		return ExpressionType::WINDOW_CUME_DIST;
	} else if (fun_name == "lead") {
		return ExpressionType::WINDOW_LEAD;
	} else if (fun_name == "lag") {
		return ExpressionType::WINDOW_LAG;
	} else if (fun_name == "ntile") {
		return ExpressionType::WINDOW_NTILE;
	}

	return ExpressionType::WINDOW_AGGREGATE;
}

void Transformer::TransformWindowDef(postgres::WindowDef *window_spec, WindowExpression *expr) {
	assert(window_spec);
	assert(expr);

	// next: partitioning/ordering expressions
	TransformExpressionList(window_spec->partitionClause, expr->partitions);
	TransformOrderBy(window_spec->orderClause, expr->orders);

	// finally: specifics of bounds
	expr->start_expr = TransformExpression(window_spec->startOffset);
	expr->end_expr = TransformExpression(window_spec->endOffset);

	if ((window_spec->frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING) ||
	    (window_spec->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)) {
		throw Exception(
		    "Window frames starting with unbounded following or ending in unbounded preceding make no sense");
	}

	if (window_spec->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) {
		expr->start = WindowBoundary::UNBOUNDED_PRECEDING;
	} else if (window_spec->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING) {
		expr->start = WindowBoundary::UNBOUNDED_FOLLOWING;
	} else if (window_spec->frameOptions & FRAMEOPTION_START_VALUE_PRECEDING) {
		expr->start = WindowBoundary::EXPR_PRECEDING;
	} else if (window_spec->frameOptions & FRAMEOPTION_START_VALUE_FOLLOWING) {
		expr->start = WindowBoundary::EXPR_FOLLOWING;
	} else if (window_spec->frameOptions & (FRAMEOPTION_START_CURRENT_ROW | FRAMEOPTION_RANGE)) {
		expr->start = WindowBoundary::CURRENT_ROW_RANGE;
	} else if (window_spec->frameOptions & (FRAMEOPTION_START_CURRENT_ROW | FRAMEOPTION_ROWS)) {
		expr->start = WindowBoundary::CURRENT_ROW_ROWS;
	}

	if (window_spec->frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING) {
		expr->end = WindowBoundary::UNBOUNDED_PRECEDING;
	} else if (window_spec->frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
		expr->end = WindowBoundary::UNBOUNDED_FOLLOWING;
	} else if (window_spec->frameOptions & FRAMEOPTION_END_VALUE_PRECEDING) {
		expr->end = WindowBoundary::EXPR_PRECEDING;
	} else if (window_spec->frameOptions & FRAMEOPTION_END_VALUE_FOLLOWING) {
		expr->end = WindowBoundary::EXPR_FOLLOWING;
	} else if (window_spec->frameOptions & (FRAMEOPTION_END_CURRENT_ROW | FRAMEOPTION_RANGE)) {
		expr->end = WindowBoundary::CURRENT_ROW_RANGE;
	} else if (window_spec->frameOptions & (FRAMEOPTION_END_CURRENT_ROW | FRAMEOPTION_ROWS)) {
		expr->end = WindowBoundary::CURRENT_ROW_ROWS;
	}

	assert(expr->start != WindowBoundary::INVALID && expr->end != WindowBoundary::INVALID);
	if (((expr->start == WindowBoundary::EXPR_PRECEDING || expr->start == WindowBoundary::EXPR_PRECEDING) &&
	     !expr->start_expr) ||
	    ((expr->end == WindowBoundary::EXPR_PRECEDING || expr->end == WindowBoundary::EXPR_PRECEDING) &&
	     !expr->end_expr)) {
		throw Exception("Failed to transform window boundary expression");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformFuncCall(postgres::FuncCall *root) {
	auto name = root->funcname;
	string schema, function_name;
	if (name->length == 2) {
		// schema + name
		schema = reinterpret_cast<postgres::Value *>(name->head->data.ptr_value)->val.str;
		function_name = reinterpret_cast<postgres::Value *>(name->head->next->data.ptr_value)->val.str;
	} else {
		// unqualified name
		schema = DEFAULT_SCHEMA;
		function_name = reinterpret_cast<postgres::Value *>(name->head->data.ptr_value)->val.str;
	}

	auto lowercase_name = StringUtil::Lower(function_name);

	if (root->over) {

		auto win_fun_type = WindowToExpressionType(lowercase_name);
		if (win_fun_type == ExpressionType::INVALID) {
			throw Exception("Unknown/unsupported window function");
		}

		auto expr = make_unique<WindowExpression>(win_fun_type, schema, lowercase_name);

		if (root->args) {
			vector<unique_ptr<ParsedExpression>> function_list;
			auto res = TransformExpressionList(root->args, function_list);
			if (!res) {
				throw Exception("Failed to transform window function children");
			}
			if (win_fun_type == ExpressionType::WINDOW_AGGREGATE) {
				for (auto& child : function_list ) {
					expr->children.push_back(move(child));
				}
			} else {
				if (function_list.size() > 0) {
					expr->children.push_back(move(function_list[0]));
				}
				if (function_list.size() > 1) {
					assert(win_fun_type == ExpressionType::WINDOW_LEAD || win_fun_type == ExpressionType::WINDOW_LAG);
				expr->offset_expr = move(function_list[1]);
				}
				if (function_list.size() > 2) {
					assert(win_fun_type == ExpressionType::WINDOW_LEAD || win_fun_type == ExpressionType::WINDOW_LAG);
				expr->default_expr = move(function_list[2]);
				}
				assert(function_list.size() <= 3);
			}
		}
		auto window_spec = reinterpret_cast<postgres::WindowDef *>(root->over);

		if (window_spec->name) {
			auto it = window_clauses.find(StringUtil::Lower(string(window_spec->name)));
			if (it == window_clauses.end()) {
				throw Exception("Could not find named window specification");
			}
			window_spec = it->second;
			assert(window_spec);
		}
		TransformWindowDef(window_spec, expr.get());

		return move(expr);
	}

	vector<unique_ptr<ParsedExpression>> children;
	if (root->args != nullptr) {
		for (auto node = root->args->head; node != nullptr; node = node->next) {
			auto child_expr = TransformExpression((postgres::Node *)node->data.ptr_value);
			children.push_back(move(child_expr));
		}
	}

	return make_unique<FunctionExpression>(schema, lowercase_name.c_str(), children, root->agg_distinct);
}

static string SQLValueOpToString(postgres::SQLValueFunctionOp op) {
	switch (op) {
	case postgres::SVFOP_CURRENT_DATE:
		return "current_date";
	case 	postgres::SVFOP_CURRENT_TIME:
		return "current_time";
	case	postgres::SVFOP_CURRENT_TIME_N:
		return "current_time_n";
	case	postgres::SVFOP_CURRENT_TIMESTAMP:
		return "current_timestamp";
	case	postgres::SVFOP_CURRENT_TIMESTAMP_N:
		return "current_timestamp_n";
	case	postgres::SVFOP_LOCALTIME:
		return "current_localtime";
	case	postgres::SVFOP_LOCALTIME_N:
		return "current_localtime_n";
	case	postgres::SVFOP_LOCALTIMESTAMP:
		return "current_localtimestamp";
	case	postgres::SVFOP_LOCALTIMESTAMP_N:
		return "current_localtimestamp_n";
	case	postgres::SVFOP_CURRENT_ROLE:
		return "current_role";
	case	postgres::SVFOP_CURRENT_USER:
		return "current_user";
	case	postgres::SVFOP_USER:
		return "user";
	case	postgres::SVFOP_SESSION_USER:
		return "session_user";
	case	postgres::SVFOP_CURRENT_CATALOG:
		return "current_catalog";
	case	postgres::SVFOP_CURRENT_SCHEMA:
		return "current_schema";
	default:
		throw Exception("Could not find named SQL value function specification " + to_string((int)op));
	}
}

unique_ptr<ParsedExpression> Transformer::TransformSQLValueFunction(postgres::SQLValueFunction *node) {
	if (!node) {
		return nullptr;
	}
	vector<unique_ptr<ParsedExpression>> children;
	auto fname = SQLValueOpToString(node->op);
	return make_unique<FunctionExpression>(DEFAULT_SCHEMA, fname, children);
}
