//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/data_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/index_type.hpp"
#include "common/types/data_chunk.hpp"
#include "storage/index.hpp"
#include "storage/table_statistics.hpp"
#include "storage/block.hpp"
#include "storage/column_data.hpp"
#include "storage/table/column_segment.hpp"
#include "storage/table/persistent_segment.hpp"
#include "storage/table/version_manager.hpp"
#include "transaction/local_storage.hpp"

#include <atomic>
#include <mutex>
#include <vector>

namespace duckdb {
class ClientContext;
class ColumnDefinition;
class StorageManager;
class TableCatalogEntry;
class Transaction;

typedef unique_ptr<vector<unique_ptr<PersistentSegment>>[]> persistent_data_t;

//! DataTable represents a physical table on disk
class DataTable {
public:
	DataTable(StorageManager &storage, string schema, string table, vector<TypeId> types,
	          persistent_data_t data);

	//! The amount of elements in the table. Note that this number signifies the amount of COMMITTED entries in the
	//! table. It can be inaccurate inside of transactions. More work is needed to properly support that.
	std::atomic<index_t> cardinality;
	// schema of the table
	string schema;
	// name of the table
	string table;
	//! Types managed by data table
	vector<TypeId> types;
	//! A reference to the base storage manager
	StorageManager &storage;
	//! Indexes
	vector<unique_ptr<Index>> indexes;

public:
	void InitializeScan(TableScanState &state, vector<column_t> column_ids);
	void InitializeScan(Transaction &transaction, TableScanState &state, vector<column_t> column_ids);
	//! Scans up to STANDARD_VECTOR_SIZE elements from the table starting
	// from offset and store them in result. Offset is incremented with how many
	// elements were returned.
	void Scan(Transaction &transaction, DataChunk &result, TableScanState &structure);
	//! Fetch data from the specific row identifiers from the base table
	void Fetch(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids, Vector &row_ids);
	//! Append a DataChunk to the table. Throws an exception if the columns
	// don't match the tables' columns.
	void Append(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk);
	//! Delete the entries with the specified row identifier from the table
	void Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_ids);
	//! Update the entries with the specified row identifier from the table
	void Update(TableCatalogEntry &table, ClientContext &context, Vector &row_ids, vector<column_t> &column_ids,
	            DataChunk &data);

	void InitializeIndexScan(IndexTableScanState &state, vector<column_t> column_ids);
	//! Scan used for creating an index, incrementally locks all storage chunks and scans ALL tuples in the table
	//! (including all versions of a tuple)
	void CreateIndexScan(IndexTableScanState &structure, DataChunk &result);

	//! Add an index to the DataTable
	void AddIndex(unique_ptr<Index> index, vector<unique_ptr<Expression>> &expressions);
public:
	//! Begin appending structs to this table, obtaining necessary locks, etc
	void InitializeAppend(TableAppendState &state);
	//! Append a chunk to the table using the AppendState obtained from BeginAppend
	void Append(Transaction &transaction, transaction_t commit_id, DataChunk &chunk, TableAppendState &state);

	//! Append a chunk with the row ids [row_start, ..., row_start + chunk.size()] to all indexes of the table, returns whether or not the append succeeded
	bool AppendToIndexes(DataChunk &chunk, row_t row_start);
	//! Remove a chunk with the row ids [row_start, ..., row_start + chunk.size()] from all indexes of the table
	void RemoveFromIndexes(DataChunk &chunk, row_t row_start);
	//! Remove the chunk with the specified set of row identifiers from all indexes of the table
	void RemoveFromIndexes(DataChunk &chunk, Vector &row_identifiers);
	//! Remove the row identifiers from all the indexes of the table
	void RemoveFromIndexes(Vector &row_identifiers);
private:
	//! Verify constraints with a chunk from the Append containing all columns of the table
	void VerifyAppendConstraints(TableCatalogEntry &table, DataChunk &chunk);
	//! Verify constraints with a chunk from the Update containing only the specified column_ids
	void VerifyUpdateConstraints(TableCatalogEntry &table, DataChunk &chunk, vector<column_t> &column_ids);

	//! Issue the specified update to the set of indexes
	void UpdateIndexes(TableCatalogEntry &table, vector<column_t> &column_ids, DataChunk &updates,
	                   Vector &row_identifiers);

private:
	//! Lock for appending entries to the table
	std::mutex append_lock;
	//! The version manager of the tree
	VersionManager version_manager;
	//! The physical columns of the table
	unique_ptr<ColumnData[]> columns;
};
} // namespace duckdb
