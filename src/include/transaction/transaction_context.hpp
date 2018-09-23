//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// transaction/transaction_context.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class Transaction;
class TransactionManager;

//! The transaction context keeps track of all the information relating to the
//! current transaction
class TransactionContext {
  public:
	TransactionContext(TransactionManager &transaction_manager)
	    : transaction_manager(transaction_manager), auto_commit(true),
	      current_transaction(nullptr) {}
	~TransactionContext();

	Transaction &ActiveTransaction() {
		assert(current_transaction);
		return *current_transaction;
	}

	bool HasActiveTransaction() { return !!current_transaction; }

	void RecordQuery(std::string query);
	void BeginTransaction();
	void Commit();
	void Rollback();

	void SetAutoCommit(bool value) { auto_commit = value; }
	bool IsAutoCommit() { return auto_commit; }

  private:
	TransactionManager &transaction_manager;
	bool auto_commit;

	Transaction *current_transaction;

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace duckdb
