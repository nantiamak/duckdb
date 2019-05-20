//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/serializer/buffered_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/serializer/buffered_file_writer.hpp"

namespace duckdb {

class BufferedFileReader : public Deserializer {
public:
	BufferedFileReader(FileSystem &fs, const char *path);

	FileSystem &fs;
	unique_ptr<data_t[]> data;
	index_t offset;
	index_t read_data;
	unique_ptr<FileHandle> handle;
public:
	void ReadData(data_ptr_t buffer, uint64_t read_size) override;
	//! Returns true if the reader has finished reading the entire file
	bool Finished();
private:
	index_t file_size;
	index_t total_read;

};

} // namespace duckdb
