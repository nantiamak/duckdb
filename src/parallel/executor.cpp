#include "duckdb/execution/executor.hpp"

#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

Executor::Executor(ClientContext &context) : context(context), finished(false) {
}

Executor::~Executor() {
}

void Executor::Initialize(unique_ptr<PhysicalOperator> plan) {
	pipelines.clear();

	physical_plan = move(plan);
	physical_state = physical_plan->GetOperatorState();

	context.profiler.Initialize(physical_plan.get());

	BuildPipelines(physical_plan.get(), nullptr);

	// schedule pipelines that do not have dependents
	for (auto &pipeline : pipelines) {
		if (!pipeline->HasDependencies()) {
			scheduled_pipelines.push(pipeline);
		}
	}

	finished = false;

	// schedule the executor so worker threads can help work on this query
	auto &scheduler = TaskScheduler::GetScheduler(context);
	scheduler.Schedule(this);

	// now work on the tasks of this pipeline until the query is finished executing
	while (pipelines.size() > 0) {
		Work();
	}

	// finished execution: unschedule the executor again
	scheduler.Finish(this);

	if (exceptions.size() > 0) {
		// an exception has occurred executing one of the pipelines
		throw Exception(exceptions[0]);
	}
}

void Executor::Work() {
	if (finished) {
		return;
	}

	while (scheduled_pipelines.size() > 0) {
		// find a pipeline to work on
		shared_ptr<Pipeline> pipeline;
		{
			lock_guard<mutex> elock(executor_lock);
			while (scheduled_pipelines.size() > 0) {
				pipeline = scheduled_pipelines.front();
				if (!pipeline->TryWork()) {
					// cannot work on this pipeline!
					scheduled_pipelines.pop();
					pipeline.reset();
				} else {
					break;
				}
			}
		}
		if (pipeline) {
			pipeline->Execute();
			pipeline->Finish();
		}
	}
}

void Executor::Reset() {
	physical_plan = nullptr;
	physical_state = nullptr;
	exceptions.clear();
}

void Executor::BuildPipelines(PhysicalOperator *op, Pipeline *parent) {
	if (op->IsSink()) {
		// operator is a sink, build a pipeline
		auto pipeline = make_shared<Pipeline>(*this);
		pipeline->sink = (PhysicalSink *)op;
		pipeline->sink_state = pipeline->sink->GetGlobalState(context);
		if (parent) {
			// the parent is dependent on this pipeline to complete
			parent->AddDependency(pipeline.get());
		}
		switch (op->type) {
		case PhysicalOperatorType::INSERT:
		case PhysicalOperatorType::DELETE:
		case PhysicalOperatorType::UPDATE:
		case PhysicalOperatorType::CREATE:
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::DISTINCT:
		case PhysicalOperatorType::SIMPLE_AGGREGATE:
		case PhysicalOperatorType::WINDOW:
		case PhysicalOperatorType::ORDER_BY:
		case PhysicalOperatorType::TOP_N:
			// single operator, set as child
			pipeline->child = op->children[0].get();
			break;
		case PhysicalOperatorType::NESTED_LOOP_JOIN:
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::HASH_JOIN:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
			// regular join, create a pipeline with RHS source that sinks into this pipeline
			pipeline->child = op->children[1].get();
			// on the LHS (probe child), we recurse with the current set of pipelines
			BuildPipelines(op->children[0].get(), parent);
			break;
		case PhysicalOperatorType::DELIM_JOIN: {
			// duplicate eliminated join
			auto &delim_join = (PhysicalDelimJoin &)*op;
			// create a pipeline with the duplicate eliminated path as source
			pipeline->child = op->children[0].get();
			// any scan of the duplicate eliminated data on the RHS depends on this pipeline
			// we add an entry to the mapping of (ChunkCollection*) -> (Pipeline*)
			delim_join_dependencies[&delim_join.delim_data] = pipeline.get();
			// recurse into the actual join; any pipelines in there depend on the main pipeline
			BuildPipelines(delim_join.join.get(), parent);
			break;
		}
		default:
			throw NotImplementedException("Unimplemented sink type!");
		}
		// recurse into the pipeline child
		BuildPipelines(pipeline->child, pipeline.get());
		pipelines.push_back(move(pipeline));
	} else {
		// operator is not a sink! recurse in children
		// first check if there is any additional action we need to do depending on the type
		switch (op->type) {
		case PhysicalOperatorType::DELIM_SCAN: {
			auto &chunk_scan = (PhysicalChunkScan &)*op;
			// check if this chunk scan scans a duplicate eliminated join collection
			auto entry = delim_join_dependencies.find(chunk_scan.collection);
			assert(entry != delim_join_dependencies.end());
			// this chunk scan introduces a dependency to the current pipeline
			// namely a dependency on the duplicate elimination pipeline to finish
			assert(parent);
			parent->AddDependency(entry->second);
			break;
		}
		case PhysicalOperatorType::EXECUTE: {
			// EXECUTE statement: build pipeline on child
			auto &execute = (PhysicalExecute &)*op;
			BuildPipelines(execute.plan, parent);
			break;
		}
		default:
			break;
		}
		for (auto &child : op->children) {
			BuildPipelines(child.get(), parent);
		}
	}
};

vector<TypeId> Executor::GetTypes() {
	assert(physical_plan);
	return physical_plan->GetTypes();
}

void Executor::PushError(std::string exception) {
	lock_guard<mutex> elock(executor_lock);
	// interrupt execution of any other pipelines that belong to this executor
	context.interrupted = true;
	// push the exception onto the stack
	exceptions.push_back(exception);
}

void Executor::Flush(ThreadContext &tcontext) {
	lock_guard<mutex> elock(executor_lock);
	context.profiler.Flush(tcontext.profiler);
}

void Executor::SchedulePipeline(shared_ptr<Pipeline> pipeline) {
	assert(!pipeline->HasDependencies());
	lock_guard<mutex> elock(executor_lock);
	scheduled_pipelines.push(move(pipeline));
}

void Executor::ErasePipeline(Pipeline *pipeline) {
	lock_guard<mutex> elock(executor_lock);
	pipelines.erase(std::find_if(pipelines.begin(), pipelines.end(),
	                             [&](shared_ptr<Pipeline> &arg) { return arg.get() == pipeline; }));
}

unique_ptr<DataChunk> Executor::FetchChunk() {
	assert(physical_plan);

	ThreadContext thread(context);
	ExecutionContext econtext(context, thread);

	auto chunk = make_unique<DataChunk>();
	// run the plan to get the next chunks
	physical_plan->InitializeChunk(*chunk);
	physical_plan->GetChunk(econtext, *chunk, physical_state.get());
	return chunk;
}

} // namespace duckdb
