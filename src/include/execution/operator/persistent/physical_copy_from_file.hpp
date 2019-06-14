//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/persistent/physical_copy_from_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "parser/parsed_data/copy_info.hpp"

namespace duckdb {
class BufferedCSVReader;

//! Parse a CSV file and return the set of chunks retrieved from the file
class PhysicalCopyFromFile : public PhysicalOperator {
public:
	PhysicalCopyFromFile(LogicalOperator &op, vector<SQLType> sql_types, unique_ptr<CopyInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::COPY_FROM_FILE, op.types), sql_types(sql_types), info(move(info)) {
	}

	//! The set of types to retrieve from the file
	vector<SQLType> sql_types;
	//! Settings for the COPY statement
	unique_ptr<CopyInfo> info;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalCopyFromFileOperatorState : public PhysicalOperatorState {
public:
	PhysicalCopyFromFileOperatorState();
	~PhysicalCopyFromFileOperatorState();

	//! The istream to read from
	unique_ptr<std::istream> csv_stream;
	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
};

} // namespace duckdb
