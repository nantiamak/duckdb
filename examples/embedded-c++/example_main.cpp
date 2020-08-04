#include "duckdb.hpp"
#include<iostream>
#include <chrono>
#include <thread>
#include <fstream>

using namespace duckdb;
using namespace std;

int main() {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE nation(n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER, n_comment VARCHAR)");
	con.Query("CREATE TABLE customer(c_custkey INTEGER, c_name VARCHAR, c_address VARCHAR, c_nationkey INTEGER, c_phone VARCHAR, c_acctbal DOUBLE, c_mktsegment VARCHAR, c_comment VARCHAR)");
	con.Query("COPY nation FROM '../../duckdb_benchmark_data/tpch_nation.csv'");
	con.Query("COPY customer FROM '../../duckdb_benchmark_data/tpch_customer.csv'");

	auto result = con.Query("select n_name, n_comment from nation where n_nationkey<6;");
	result->Print();
}
