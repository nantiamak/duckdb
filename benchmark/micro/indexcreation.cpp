
#include "benchmark_runner.hpp"
#include "duckdb_benchmark.hpp"
#include <random>

using namespace duckdb;
using namespace std;

#define ROW_COUNT 100000000
#define UPPERBOUND 100000000
#define SUCCESS 0

DUCKDB_BENCHMARK(IndexCreationUniformRandomData, "[micro]")
    virtual void Load(DuckDBBenchmarkState *state) {
        state->conn.Query("CREATE TABLE integers(i INTEGER);");
        auto appender = state->conn.GetAppender("integers");
        // insert the elements into the database
        for (size_t i = 0; i < ROW_COUNT; i++) {
            appender->begin_append_row();
            appender->append_int(rand()%UPPERBOUND);
            appender->end_append_row();
        }
        state->conn.DestroyAppender();
    }

    virtual std::string GetQuery() {
        return "CREATE INDEX i_index ON integers(i)";
    }

    virtual void Cleanup(DuckDBBenchmarkState *state){
        state->conn.Query("DROP INDEX i_index;");
    }

    virtual std::string VerifyResult(DuckDBResult *result) {
        if (!result->GetSuccess()) {
            return result->GetErrorMessage();
        }
        if (result->size() != 1) {
            return "Incorrect amount of rows in result";
        }
        if (result->column_count() != 1) {
            return "Incorrect amount of columns";
        }
        if (result->GetValue<int>(0, 0) != SUCCESS) {
            return "Incorrect result returned, expected " +
                    result->GetValue<int>(0, 0) ;
        }
        return std::string();
    }

    virtual std::string BenchmarkInfo() {
        return StringUtil::Format("Creates an Index on a Uniform Random Column");
    }

FINISH_BENCHMARK(IndexCreationUniformRandomData)
