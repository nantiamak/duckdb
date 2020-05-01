#include "duckdb/execution/operator/persistent/physical_insert.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class InsertGlobalState : public GlobalOperatorState {
public:
	InsertGlobalState() : insert_count(0) {
	}

	std::mutex lock;
	idx_t insert_count;
};

class InsertLocalState : public LocalSinkState {
public:
	InsertLocalState(vector<TypeId> types, vector<unique_ptr<Expression>> &bound_defaults) : default_executor(bound_defaults) {
		insert_chunk.Initialize(types);
	}

	DataChunk insert_chunk;
	ExpressionExecutor default_executor;
};

void PhysicalInsert::Sink(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &chunk) {
	auto &gstate = (InsertGlobalState &) state;
	auto &istate = (InsertLocalState &) lstate;

	chunk.Normalify();
	istate.default_executor.SetChunk(chunk);

	istate.insert_chunk.Reset();
	istate.insert_chunk.SetCardinality(chunk);
	if (column_index_map.size() > 0) {
		// columns specified by the user, use column_index_map
		for (idx_t i = 0; i < table->columns.size(); i++) {
			if (column_index_map[i] == INVALID_INDEX) {
				// insert default value
				istate.default_executor.ExecuteExpression(i, istate.insert_chunk.data[i]);
			} else {
				// get value from child chunk
				assert((idx_t)column_index_map[i] < chunk.column_count());
				assert(istate.insert_chunk.data[i].type == chunk.data[column_index_map[i]].type);
				istate.insert_chunk.data[i].Reference(chunk.data[column_index_map[i]]);
			}
		}
	} else {
		// no columns specified, just append directly
		for (idx_t i = 0; i < istate.insert_chunk.column_count(); i++) {
			assert(istate.insert_chunk.data[i].type == chunk.data[i].type);
			istate.insert_chunk.data[i].Reference(chunk.data[i]);
		}
	}

	lock_guard<mutex> glock(gstate.lock);
	table->storage->Append(*table, context, istate.insert_chunk);
	gstate.insert_count += chunk.size();
}

unique_ptr<GlobalOperatorState> PhysicalInsert::GetGlobalState(ClientContext &context) {
	return make_unique<InsertGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalInsert::GetLocalSinkState(ClientContext &context) {
	return make_unique<InsertLocalState>(table->GetTypes(), bound_defaults);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
void PhysicalInsert::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &gstate = (InsertGlobalState &) *sink_state;

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(gstate.insert_count));

	state->finished = true;
}

}
