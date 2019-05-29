//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb.h
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t index_t;
typedef uint64_t count_t;

typedef enum DUCKDB_TYPE {
	DUCKDB_TYPE_INVALID = 0,
	// bool
	DUCKDB_TYPE_BOOLEAN,
	// int8_t
	DUCKDB_TYPE_TINYINT,
	// int16_t
	DUCKDB_TYPE_SMALLINT,
	// int32_t
	DUCKDB_TYPE_INTEGER,
	// int64_t
	DUCKDB_TYPE_BIGINT,
	// float
	DUCKDB_TYPE_FLOAT,
	// double
	DUCKDB_TYPE_DOUBLE,
	// duckdb_timestamp
	DUCKDB_TYPE_TIMESTAMP,
	// duckdb_date
	DUCKDB_TYPE_DATE,
	// const char*
	DUCKDB_TYPE_VARCHAR
} duckdb_type;

typedef struct {
	int16_t year;
	int16_t month;
	int16_t day;
} duckdb_date;

typedef struct {
	int16_t hour;
	int16_t min;
	int16_t sec;
	int16_t msec;
} duckdb_time;

typedef struct {
	duckdb_date date;
	duckdb_time time;
} duckdb_timestamp;

typedef struct {
	void *data;
	bool *nullmask;
	duckdb_type type;
	char *name;
} duckdb_column;

typedef struct {
	count_t column_count;
	count_t row_count;
	duckdb_column *columns;
	char *error_message;
} duckdb_result;

// typedef struct {
// 	void *data;
// 	bool *nullmask;
// } duckdb_column_data;

// typedef struct {
// 	int column_count;
// 	int count;
// 	duckdb_column_data *columns;
// } duckdb_chunk;

typedef void *duckdb_database;
typedef void *duckdb_connection;

typedef enum { DuckDBSuccess = 0, DuckDBError = 1 } duckdb_state;

//! Opens a database file at the given path (nullptr for in-memory). Returns DuckDBSuccess on success, or DuckDBError on
//! failure. [OUT: database]
duckdb_state duckdb_open(const char *path, duckdb_database *out_database);
//! Closes the database.
void duckdb_close(duckdb_database *database);

//! Creates a connection to the specified database. [OUT: connection]
duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection);
//! Closes the specified connection handle
void duckdb_disconnect(duckdb_connection *connection);

//! Executes the specified SQL query in the specified connection handle. [OUT: result descriptor]
duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result);
//! Destroys the specified result
void duckdb_destroy_result(duckdb_result *result);

// SAFE fetch functions
// These functions will perform conversions if necessary. On failure (e.g. if conversion cannot be performed) a special
// value is returned.

//! Converts the specified value to a bool. Returns false on failure or NULL.
bool duckdb_value_boolean(duckdb_result *result, index_t col, index_t row);
//! Converts the specified value to an int8_t. Returns 0 on failure or NULL.
int8_t duckdb_value_int8(duckdb_result *result, index_t col, index_t row);
//! Converts the specified value to an int16_t. Returns 0 on failure or NULL.
int16_t duckdb_value_int16(duckdb_result *result, index_t col, index_t row);
//! Converts the specified value to an int64_t. Returns 0 on failure or NULL.
int32_t duckdb_value_int32(duckdb_result *result, index_t col, index_t row);
//! Converts the specified value to an int64_t. Returns 0 on failure or NULL.
int64_t duckdb_value_int64(duckdb_result *result, index_t col, index_t row);
//! Converts the specified value to a float. Returns 0.0 on failure or NULL.
float duckdb_value_float(duckdb_result *result, index_t col, index_t row);
//! Converts the specified value to a double. Returns 0.0 on failure or NULL.
double duckdb_value_double(duckdb_result *result, index_t col, index_t row);
//! Converts the specified value to a string. Returns nullptr on failure or NULL. The result must be freed with free.
char *duckdb_value_varchar(duckdb_result *result, index_t col, index_t row);

#ifdef __cplusplus
};
#endif
