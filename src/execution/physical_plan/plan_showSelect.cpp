#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/parsed_data/show_select_info.hpp"
#include "duckdb/planner/operator/logical_show.hpp"

#include <iostream>

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalShow &op) {

  cout << "Inside plan show\n";
  DataChunk output;
  vector<TypeId> output_types(6, TypeId::VARCHAR);
  output.Initialize(output_types);
  idx_t offset = 0;
  cout << "Types size: " << op.types_select.size() << "\n";
	/*if (offset >= op.types_select.size()) {
		// finished returning values
		return;
	}*/
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t next = min(offset + STANDARD_VECTOR_SIZE, (idx_t)op.types_select.size());
	output.SetCardinality(next - offset);

	for (idx_t i = offset; i < next; i++) {
		auto index = i - offset;
		auto type = op.types_select[index];
		auto &name = op.aliases[index];
		// return values:
		// "cid", TypeId::INT32

		output.SetValue(0, index, Value::INTEGER((int32_t)index));
		// "name", TypeId::VARCHAR
		output.SetValue(1, index, Value(name));
		// "type", TypeId::VARCHAR
		output.SetValue(2, index, Value(SQLTypeToString(type)));
		// "notnull", TypeId::BOOL
		output.SetValue(3, index, Value::BOOLEAN(false));
		// "dflt_value", TypeId::VARCHAR
		output.SetValue(4, index, Value());
		// "pk", TypeId::BOOL
		output.SetValue(5, index, Value::BOOLEAN(false));
	}
	offset = next;

  auto collection = make_unique<ChunkCollection>();

  collection->Append(output);

	// create a chunk scan to output the result
	auto chunk_scan = make_unique<PhysicalChunkScan>(output_types, PhysicalOperatorType::CHUNK_SCAN);
	chunk_scan->owned_collection = move(collection);
	chunk_scan->collection = chunk_scan->owned_collection.get();
	return move(chunk_scan);
}
