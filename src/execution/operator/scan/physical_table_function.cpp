#include "execution/operator/scan/physical_table_function.hpp"

#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"
#include "planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

void PhysicalTableFunction::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = (PhysicalTableFunctionOperatorState *)state_;
	if (!state->initialized) {
		// run initialization code
		if (function->init) {
			auto function_data = function->init(context);
			if (function_data) {
				state->function_data = unique_ptr<TableFunctionData>(function_data);
			}
		}
		state->initialized = true;
	}
	// create the input arguments
	vector<TypeId> input_types;
	for (auto &argument_type : function->arguments) {
		input_types.push_back(GetInternalType(argument_type));
	}

	DataChunk input;
	input.Initialize(input_types);

	ExpressionExecutor executor;
	assert(function_call->type == ExpressionType::FUNCTION);
	executor.Execute(((BoundFunctionExpression *)function_call.get())->children, input);

	// run main code
	function->function(context, input, chunk, state->function_data.get());
	if (chunk.size() == 0) {
		// finished, call clean up
		if (function->final) {
			function->final(context, state->function_data.get());
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalTableFunction::GetOperatorState() {
	return make_unique<PhysicalTableFunctionOperatorState>();
}
