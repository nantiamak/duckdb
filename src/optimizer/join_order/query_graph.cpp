#include "duckdb/optimizer/join_order/query_graph.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

using QueryEdge = QueryGraph::QueryEdge;

static string QueryEdgeToString(const QueryEdge *info, vector<index_t> prefix) {
	string result = "";
	string source = "[";
	for (index_t i = 0; i < prefix.size(); i++) {
		source += to_string(prefix[i]) + (i < prefix.size() - 1 ? ", " : "");
	}
	source += "]";
	for (auto &entry : info->neighbors) {
		result += StringUtil::Format("%s -> %s\n", source.c_str(), entry->neighbor->ToString().c_str());
	}
	for (auto &entry : info->children) {
		vector<index_t> new_prefix = prefix;
		new_prefix.push_back(entry.first);
		result += QueryEdgeToString(entry.second.get(), new_prefix);
	}
	return result;
}

string QueryGraph::ToString() const {
	return QueryEdgeToString(&root, {});
}

QueryEdge *QueryGraph::GetQueryEdge(RelationSet *left) {
	assert(left && left->count > 0);
	// find the EdgeInfo corresponding to the left set
	QueryEdge *info = &root;
	for (index_t i = 0; i < left->count; i++) {
		auto entry = info->children.find(left->relations[i]);
		if (entry == info->children.end()) {
			// node not found, create it
			auto insert_it = info->children.insert(make_pair(left->relations[i], make_unique<QueryEdge>()));
			entry = insert_it.first;
		}
		// move to the next node
		info = entry->second.get();
	}
	return info;
}

void QueryGraph::CreateEdge(RelationSet *left, RelationSet *right, FilterInfo *filter_info) {
	assert(left && right && left->count > 0 && right->count > 0);
	// find the EdgeInfo corresponding to the left set
	auto info = GetQueryEdge(left);
	// now insert the edge to the right relation, if it does not exist
	for (index_t i = 0; i < info->neighbors.size(); i++) {
		if (info->neighbors[i]->neighbor == right) {
			if (filter_info) {
				// neighbor already exists just add the filter, if we have any
				info->neighbors[i]->filters.push_back(filter_info);
			}
			return;
		}
	}
	// neighbor does not exist, create it
	auto n = make_unique<NeighborInfo>();
	if (filter_info) {
		n->filters.push_back(filter_info);
	}
	n->neighbor = right;
	info->neighbors.push_back(move(n));
}

void QueryGraph::EnumerateNeighbors(RelationSet *node, function<bool(NeighborInfo *)> callback) {
	for (index_t j = 0; j < node->count; j++) {
		QueryEdge *info = &root;
		for (index_t i = j; i < node->count; i++) {
			auto entry = info->children.find(node->relations[i]);
			if (entry == info->children.end()) {
				// node not found
				break;
			}
			// check if any subset of the other set is in this sets neighbors
			info = entry->second.get();
			for (auto &neighbor : info->neighbors) {
				if (callback(neighbor.get())) {
					return;
				}
			}
		}
	}
}

//! Returns true if a RelationSet is banned by the list of exclusion_set, false otherwise
static bool RelationSetIsExcluded(RelationSet *node, unordered_set<index_t> &exclusion_set) {
	return exclusion_set.find(node->relations[0]) != exclusion_set.end();
}

vector<index_t> QueryGraph::GetNeighbors(RelationSet *node, unordered_set<index_t> &exclusion_set) {
	unordered_set<index_t> result;
	EnumerateNeighbors(node, [&](NeighborInfo *info) -> bool {
		if (!RelationSetIsExcluded(info->neighbor, exclusion_set)) {
			// add the smallest node of the neighbor to the set
			result.insert(info->neighbor->relations[0]);
		}
		return false;
	});
	vector<index_t> neighbors;
	neighbors.insert(neighbors.end(), result.begin(), result.end());
	return neighbors;
}

NeighborInfo *QueryGraph::GetConnection(RelationSet *node, RelationSet *other) {
	NeighborInfo *connection = nullptr;
	EnumerateNeighbors(node, [&](NeighborInfo *info) -> bool {
		if (RelationSet::IsSubset(other, info->neighbor)) {
			connection = info;
			return true;
		}
		return false;
	});
	return connection;
}

void QueryGraph::Print() {
	Printer::Print(ToString());
}
