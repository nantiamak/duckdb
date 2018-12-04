#include "main/connection.hpp"

#include "execution/executor.hpp"
#include "execution/physical_plan_generator.hpp"
#include "main/database.hpp"
#include "optimizer/optimizer.hpp"
#include "parser/parser.hpp"
#include "planner/planner.hpp"

using namespace duckdb;
using namespace std;

DuckDBConnection::DuckDBConnection(DuckDB &database) : db(database), context(database) {
}

DuckDBConnection::~DuckDBConnection() {
}

Appender *DuckDBConnection::GetAppender(string table_name, string schema) {
	if (appender) {
		throw Exception("Connection can only have one appender active at a time!");
	}
	// start a transaction if none is active at this point
	if (context.transaction.IsAutoCommit()) {
		context.transaction.BeginTransaction();
	}
	// get the table catalog entry
	TableCatalogEntry *table_entry;
	try {
		table_entry = db.catalog.GetTable(context.transaction.ActiveTransaction(), schema, table_name);
	} catch (...) {
		if (context.transaction.IsAutoCommit()) {
			context.transaction.Rollback();
		}
		throw;
	}
	// found the table, create the appender for the table
	this->appender = make_unique<Appender>(*this, context, table_entry);
	return appender.get();
}

void DuckDBConnection::DestroyAppender(bool rollback) {
	if (!appender) {
		throw Exception("No appender active to destroy");
	}
	this->appender->flush();
	this->appender = nullptr;
	if (context.transaction.IsAutoCommit()) {
		if (!rollback) {
			context.transaction.Commit();
		} else {
			context.transaction.Rollback();
		}
	}
}

unique_ptr<DuckDBResult> DuckDBConnection::GetQueryResult(ClientContext &context, std::string query) {
	auto result = make_unique<DuckDBResult>();
	result->success = false;

	context.profiler.StartQuery(query);
	context.interrupted = false;
	try {
		// parse the query and transform it into a set of statements
		Parser parser;
		parser.ParseQuery(query.c_str());
		if (parser.statements.size() == 0) {
			// empty query
			return make_unique<DuckDBResult>();
		}

		if (parser.statements.size() > 1) {
			throw Exception("More than one statement per query not supported yet!");
		}

		auto &statement = parser.statements.back();
		if (statement->type == StatementType::UPDATE || statement->type == StatementType::DELETE ||
		    statement->type == StatementType::ALTER || statement->type == StatementType::CREATE_INDEX) {
			// log query in UNDO buffer so it can be saved in the WAL on commit
			auto &transaction = context.transaction.ActiveTransaction();
			transaction.PushQuery(query);
		}

		Planner planner;
		planner.CreatePlan(context, move(statement));
		if (!planner.plan) {
			return make_unique<DuckDBResult>();
		}

		auto plan = move(planner.plan);
		Optimizer optimizer(*planner.context);
		plan = optimizer.Optimize(move(plan));
		if (!plan) {
			return make_unique<DuckDBResult>();
		}

		// extract the result column names from the plan
		result->names = plan->GetNames();

		// now convert logical query plan into a physical query plan
		PhysicalPlanGenerator physical_planner(context);
		physical_planner.CreatePlan(move(plan));

		// finally execute the plan and return the result
		Executor executor;
		result->collection = executor.Execute(context, move(physical_planner.plan));
		result->success = true;
	} catch (Exception &ex) {
		result->error = ex.GetMessage();
	} catch (...) {
		result->error = "UNHANDLED EXCEPTION TYPE THROWN!";
	}
	context.profiler.EndQuery();
	// destroy any data held in the query allocator
	context.allocator.Destroy();
	return result;
}

unique_ptr<DuckDBResult> DuckDBConnection::GetQueryResult(std::string query) {
	return GetQueryResult(context, query);
}

unique_ptr<DuckDBResult> DuckDBConnection::Query(std::string query) {
	if (appender) {
		throw Exception("Cannot query connection while an appender is active!");
	}

	if (context.transaction.IsAutoCommit()) {
		context.transaction.BeginTransaction();
	}

	context.ActiveTransaction().active_query = context.db.transaction_manager.GetQueryNumber();
	auto result = GetQueryResult(query);

	if (context.transaction.HasActiveTransaction()) {
		context.ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		try {
			if (context.transaction.IsAutoCommit()) {
				if (result->GetSuccess()) {
					context.transaction.Commit();
				} else {
					context.transaction.Rollback();
				}
			}
		} catch (Exception &ex) {
			result->success = false;
			result->error = ex.GetMessage();
		} catch (...) {
			result->success = false;
			result->error = "UNHANDLED EXCEPTION TYPE THROWN IN TRANSACTION COMMIT!";
		}
	}
	return result;
}
