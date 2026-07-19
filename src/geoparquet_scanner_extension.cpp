#define DUCKDB_EXTENSION_MAIN

#include "geoparquet_scanner_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/function/table_function.hpp"
#include "parquet_reader.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include <iostream>
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>
#include "yyjson.hpp"

namespace duckdb {

inline void GeoparquetScannerScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "...........🦆 " + name.GetString());
	});
}

inline void GeoparquetScannerOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "GeoparquetScanner " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

struct ReadGeoParquetBindData : public TableFunctionData {
	string geometry_column_name;
	string file_path;
	bool has_covering = false;
	vector<vector<string>> bbox_paths;
	idx_t initial_file_cardinality = 0;
	bool has_global_bbox = false;
	vector<double> global_bbox;
	duckdb::TableFilterSet pushed_filters;
};

static unique_ptr<FunctionData> ReadGeoParquetBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto file_path = input.inputs[0].GetValue<string>();
	duckdb::ParquetOptions parquet_options;
	auto reader = make_shared_ptr<duckdb::ParquetReader>(context, file_path, parquet_options);
	auto metadata = reader->GetFileMetadata();
	string geometry_column_name = "geometry";
	bool found_geo = false;
	bool has_covering = false;
	vector<vector<string>> bbox_paths(4);
	bool has_global_bbox = false;
	vector<double> global_bbox;

	if (metadata) {
		for (auto &kv : metadata->key_value_metadata) {
			if (kv.key == "geo") {
				found_geo = true;
				
				duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(kv.value.c_str(), kv.value.size(), 0);
				if (doc) {
					duckdb_yyjson::yyjson_val *root = duckdb_yyjson::yyjson_doc_get_root(doc);
					duckdb_yyjson::yyjson_val *primary_col_val = duckdb_yyjson::yyjson_obj_get(root, "primary_column");
					if (primary_col_val && duckdb_yyjson::yyjson_is_str(primary_col_val)) {
						geometry_column_name = duckdb_yyjson::yyjson_get_str(primary_col_val);
					}

					duckdb_yyjson::yyjson_val *columns_obj = duckdb_yyjson::yyjson_obj_get(root, "columns");
					if (columns_obj) {
						duckdb_yyjson::yyjson_val *geom_col = duckdb_yyjson::yyjson_obj_get(columns_obj, geometry_column_name.c_str());
						if (geom_col) {
							duckdb_yyjson::yyjson_val *covering = duckdb_yyjson::yyjson_obj_get(geom_col, "covering");
							if (covering && duckdb_yyjson::yyjson_is_obj(covering)) {
								duckdb_yyjson::yyjson_val *covering_bbox = duckdb_yyjson::yyjson_obj_get(covering, "bbox");
								if (covering_bbox && duckdb_yyjson::yyjson_is_obj(covering_bbox)) {
									has_covering = true;
									auto extract_path = [](duckdb_yyjson::yyjson_val *obj, const char *key, vector<string> &path) {
										duckdb_yyjson::yyjson_val *arr = duckdb_yyjson::yyjson_obj_get(obj, key);
										if (arr && duckdb_yyjson::yyjson_is_arr(arr)) {
											size_t arr_idx, arr_max;
											duckdb_yyjson::yyjson_val *val;
											yyjson_arr_foreach(arr, arr_idx, arr_max, val) {
												if (duckdb_yyjson::yyjson_is_str(val)) {
													path.push_back(duckdb_yyjson::yyjson_get_str(val));
												}
											}
										}
									};
									extract_path(covering_bbox, "xmin", bbox_paths[0]);
									extract_path(covering_bbox, "ymin", bbox_paths[1]);
									extract_path(covering_bbox, "xmax", bbox_paths[2]);
									extract_path(covering_bbox, "ymax", bbox_paths[3]);
								}
							}
							
							duckdb_yyjson::yyjson_val *geo_bbox = duckdb_yyjson::yyjson_obj_get(geom_col, "bbox");
							if (geo_bbox && duckdb_yyjson::yyjson_is_arr(geo_bbox)) {
								size_t bbox_len = duckdb_yyjson::yyjson_arr_size(geo_bbox);
								if (bbox_len == 4 || bbox_len == 6) {
									has_global_bbox = true;
									global_bbox.push_back(duckdb_yyjson::yyjson_get_num(duckdb_yyjson::yyjson_arr_get(geo_bbox, 0))); // xmin
									global_bbox.push_back(duckdb_yyjson::yyjson_get_num(duckdb_yyjson::yyjson_arr_get(geo_bbox, 1))); // ymin
									global_bbox.push_back(duckdb_yyjson::yyjson_get_num(duckdb_yyjson::yyjson_arr_get(geo_bbox, bbox_len / 2))); // xmax
									global_bbox.push_back(duckdb_yyjson::yyjson_get_num(duckdb_yyjson::yyjson_arr_get(geo_bbox, bbox_len / 2 + 1))); // ymax
								}
							}
						}
					}
					duckdb_yyjson::yyjson_doc_free(doc);
				}
				break;
			}
		}
	}

	if (!found_geo) {
		throw duckdb::IOException("File %s does not contain 'geo' metadata, not a valid GeoParquet file", file_path);
	}

	auto union_data = reader->GetUnionData(0);
	for (idx_t i = 0; i < union_data->names.size(); i++) {
		names.emplace_back(union_data->names[i]);
		return_types.emplace_back(union_data->types[i]);
	}

	if (!has_covering) {
		for (idx_t i = 0; i < union_data->names.size(); i++) {
			if (union_data->names[i] == "bbox") {
				has_covering = true;
				bbox_paths[0] = {"bbox", "xmin"};
				bbox_paths[1] = {"bbox", "ymin"};
				bbox_paths[2] = {"bbox", "xmax"};
				bbox_paths[3] = {"bbox", "ymax"};
				break;
			}
		}
	}

	auto bind_data = make_uniq<ReadGeoParquetBindData>();
	bind_data->geometry_column_name = geometry_column_name;
	bind_data->file_path = file_path;
	bind_data->has_covering = has_covering;
	bind_data->bbox_paths = bbox_paths;
	bind_data->has_global_bbox = has_global_bbox;
	bind_data->global_bbox = global_bbox;
	bind_data->initial_file_cardinality = reader->NumRows();

	std::cout << "DEBUG: [GeoParquet] Global BBox found: " << (bind_data->has_global_bbox ? "YES" : "NO") << std::endl;
	if (bind_data->has_global_bbox) {
		std::cout << "DEBUG: [GeoParquet] Bounds: " << bind_data->global_bbox[0] << ", " << bind_data->global_bbox[2] << std::endl;
	}

	return std::move(bind_data);
}

struct ReadGeoParquetGlobalState : public GlobalTableFunctionState {
	shared_ptr<duckdb::ParquetReader> reader;
	unique_ptr<duckdb::ParquetReaderScanState> scan_state;
};

static unique_ptr<GlobalTableFunctionState> ReadGeoParquetInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadGeoParquetBindData>();
	auto global_state = make_uniq<ReadGeoParquetGlobalState>();

	// Add this check immediately inside InitGlobal:
	std::cout << "DEBUG: InitGlobal filters size: " << bind_data.pushed_filters.filters.size() << std::endl;
	std::cout << "DEBUG: Created new GlobalState at: " << global_state.get() << std::endl;

	duckdb::ParquetOptions parquet_options;
	global_state->reader = make_shared_ptr<duckdb::ParquetReader>(context, bind_data.file_path, parquet_options);
	
	global_state->scan_state = make_uniq<duckdb::ParquetReaderScanState>();

	auto union_data = global_state->reader->GetUnionData(0);
	for (auto &col_id : input.column_ids) {
		if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
			global_state->reader->column_indexes.emplace_back(duckdb::ColumnIndex());
			global_state->reader->column_ids.push_back(duckdb::MultiFileLocalColumnId(col_id));
			continue;
		}
		global_state->reader->column_indexes.emplace_back(duckdb::ColumnIndex(col_id));
		global_state->reader->column_ids.push_back(duckdb::MultiFileLocalColumnId(col_id));
	}

	vector<idx_t> groups;
	if (!bind_data.has_covering || bind_data.pushed_filters.filters.empty()) {
		for (idx_t i = 0; i < global_state->reader->NumRowGroups(); i++) {
			groups.push_back(i);
		}
	} else {
		auto get_schema_node = [](const duckdb::ParquetColumnSchema *root, const vector<string> &path) -> const duckdb::ParquetColumnSchema* {
			const duckdb::ParquetColumnSchema *current = root;
			for (const auto &p : path) {
				bool found = false;
				for (const auto &child : current->children) {
					if (child.name == p) {
						current = &child;
						found = true;
						break;
					}
				}
				if (!found) return nullptr;
			}
			return current;
		};

		const duckdb::ParquetColumnSchema *bbox_nodes[4] = {nullptr, nullptr, nullptr, nullptr};
		if (global_state->reader->root_schema) {
			for (int j = 0; j < 4; j++) {
				bbox_nodes[j] = get_schema_node(global_state->reader->root_schema.get(), bind_data.bbox_paths[j]);
			}
		}

		auto union_data = global_state->reader->GetUnionData(0);
		auto file_metadata = global_state->reader->GetFileMetadata();
		duckdb::ParquetOptions parquet_options;

		if (bind_data.has_global_bbox && !bind_data.pushed_filters.filters.empty()) {
			bool prune_entire_file = false;
			for (auto &filter_entry : bind_data.pushed_filters.filters) {
				idx_t table_col_idx = filter_entry.first;
				auto &filter = filter_entry.second;
				if (table_col_idx >= union_data->names.size()) continue;
				string filter_col_name = union_data->names[table_col_idx];
				
				std::function<bool(const duckdb::TableFilter*, const string&)> check_global_filter = 
					[&](const duckdb::TableFilter* current_filter, const string& current_col_name) -> bool {
					std::cout << "DEBUG: Filter ID: " << (int)current_filter->filter_type << " on col: " << current_col_name << std::endl;
					if (current_filter->filter_type == duckdb::TableFilterType::STRUCT_EXTRACT) {
						auto &struct_filter = current_filter->Cast<duckdb::StructFilter>();
						return check_global_filter(struct_filter.child_filter.get(), struct_filter.child_name);
					} else if (current_filter->filter_type == duckdb::TableFilterType::CONJUNCTION_AND) {
						auto &and_filter = current_filter->Cast<duckdb::ConjunctionAndFilter>();
						for (auto &child : and_filter.child_filters) {
							if (check_global_filter(child.get(), current_col_name)) return true;
						}
						return false;
					} else if (current_filter->filter_type == duckdb::TableFilterType::CONJUNCTION_OR) {
						auto &or_filter = current_filter->Cast<duckdb::ConjunctionOrFilter>();
						for (auto &child : or_filter.child_filters) {
							if (!check_global_filter(child.get(), current_col_name)) return false;
						}
						return true;
					} else {
						int global_idx = -1;
						if (current_col_name == "xmin" || current_col_name == "xmax") global_idx = 0;
						else if (current_col_name == "ymin" || current_col_name == "ymax") global_idx = 1;

						if (global_idx != -1) {
							auto stats = duckdb::NumericStats::CreateEmpty(duckdb::LogicalType::DOUBLE);
							stats.SetHasNoNull();
							if (global_idx == 0) { // xmin or xmax
								duckdb::NumericStats::SetMin(stats, duckdb::Value::DOUBLE(bind_data.global_bbox[0]));
								duckdb::NumericStats::SetMax(stats, duckdb::Value::DOUBLE(bind_data.global_bbox[2]));
								std::cout << "DEBUG: Pruning Eval: Min=" << bind_data.global_bbox[0] << ", Max=" << bind_data.global_bbox[2] << " for filter on " << current_col_name << std::endl;
							} else { // ymin or ymax
								duckdb::NumericStats::SetMin(stats, duckdb::Value::DOUBLE(bind_data.global_bbox[1]));
								duckdb::NumericStats::SetMax(stats, duckdb::Value::DOUBLE(bind_data.global_bbox[3]));
								std::cout << "DEBUG: Pruning Eval: Min=" << bind_data.global_bbox[1] << ", Max=" << bind_data.global_bbox[3] << " for filter on " << current_col_name << std::endl;
							}
							
							std::cout << "DEBUG: Filter type reached base case: " << (int)current_filter->filter_type << std::endl;
							if (current_filter->filter_type == duckdb::TableFilterType::CONSTANT_COMPARISON) {
								auto &const_filt = current_filter->Cast<duckdb::ConstantFilter>();
								duckdb::FilterPropagateResult res = current_filter->CheckStatistics(stats);
								if (res == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE) {
									return true; // Prune entirely
								}
							}
						}
						return false;
					}
				};
				
				std::cout << "DEBUG: Checking filter_col_name: '" << filter_col_name << "'" << std::endl;
				std::cout << "DEBUG: Geometry column name: '" << bind_data.geometry_column_name << "'" << std::endl;

				if (filter_col_name == "bbox" || filter_col_name == bind_data.geometry_column_name) {
					if (check_global_filter(filter.get(), filter_col_name)) {
						prune_entire_file = true;
						break;
					}
				}
			}

			if (prune_entire_file) {
				vector<idx_t> empty_groups;
				global_state->reader->InitializeScan(context, *global_state->scan_state, empty_groups);
				return std::move(global_state);
			}
		}

		for (idx_t i = 0; i < global_state->reader->NumRowGroups(); i++) {
			bool prune = false;
			const auto &row_group = file_metadata->row_groups[i];

			for (auto &filter_entry : bind_data.pushed_filters.filters) {
				idx_t table_col_idx = filter_entry.first;
				auto &filter = filter_entry.second;
				if (table_col_idx >= union_data->names.size()) continue;
				string filter_col_name = union_data->names[table_col_idx];
				
				
				std::function<bool(const duckdb::TableFilter*, const string&)> check_filter = 
				    [&](const duckdb::TableFilter* current_filter, const string& current_col_name) -> bool {
					if (current_filter->filter_type == duckdb::TableFilterType::STRUCT_EXTRACT) {
						auto &struct_filter = current_filter->Cast<duckdb::StructFilter>();
						return check_filter(struct_filter.child_filter.get(), struct_filter.child_name);
					} else if (current_filter->filter_type == duckdb::TableFilterType::CONJUNCTION_AND) {
						auto &and_filter = current_filter->Cast<duckdb::ConjunctionAndFilter>();
						for (auto &child : and_filter.child_filters) {
							if (check_filter(child.get(), current_col_name)) return true;
						}
						return false;
					} else if (current_filter->filter_type == duckdb::TableFilterType::CONJUNCTION_OR) {
						auto &or_filter = current_filter->Cast<duckdb::ConjunctionOrFilter>();
						for (auto &child : or_filter.child_filters) {
							if (!check_filter(child.get(), current_col_name)) return false;
						}
						return true;
					} else {
						const duckdb::ParquetColumnSchema *target_node = nullptr;
						for (int j = 0; j < 4; j++) {
							if (bbox_nodes[j] && !bind_data.bbox_paths[j].empty()) {
								if (current_col_name == bind_data.bbox_paths[j].back() || current_col_name == bind_data.bbox_paths[j].front()) {
									target_node = bbox_nodes[j];
									break;
								}
							}
						}
						if (target_node) {
							auto stats = target_node->Stats(*file_metadata, parquet_options, i, row_group.columns);
							if (stats) {
								auto result = current_filter->CheckStatistics(*stats);
								if (result == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE) {
									return true;
								}
							}
						}
						return false;
					}
				};
				
				std::cout << "DEBUG: Checking filter_col_name: '" << filter_col_name << "'" << std::endl;
				std::cout << "DEBUG: Geometry column name: '" << bind_data.geometry_column_name << "'" << std::endl;

				if (filter_col_name == "bbox" || filter_col_name == bind_data.geometry_column_name) {
					if (check_filter(filter.get(), filter_col_name)) {
						prune = true;
						break;
					}
				}
			}
			if (!prune) {
				groups.push_back(i);
			}
		}
	}

	if (input.filters && !input.filters->filters.empty()) {
		std::function<bool(const duckdb::TableFilter*)> contains_struct_filter = [&](const duckdb::TableFilter *f) {
			if (!f) return false;
			if (f->filter_type == duckdb::TableFilterType::STRUCT_EXTRACT) return true;
			if (f->filter_type == duckdb::TableFilterType::CONJUNCTION_AND) {
				auto &cand = f->Cast<duckdb::ConjunctionAndFilter>();
				for (auto &child : cand.child_filters) {
					if (contains_struct_filter(child.get())) return true;
				}
			}
			if (f->filter_type == duckdb::TableFilterType::CONJUNCTION_OR) {
				auto &cor = f->Cast<duckdb::ConjunctionOrFilter>();
				for (auto &child : cor.child_filters) {
					if (contains_struct_filter(child.get())) return true;
				}
			}
			return false;
		};

		global_state->reader->filters = make_uniq<duckdb::TableFilterSet>();
	}
	global_state->reader->InitializeScan(context, *global_state->scan_state, groups);

	return std::move(global_state);
}

static void ReadGeoParquetScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<ReadGeoParquetGlobalState>();
	
	if (global_state.scan_state->finished) {
		output.SetCardinality(0);
		return;
	}

	do {
		output.Reset();
		global_state.reader->Scan(context, *global_state.scan_state, output);
	} while (output.size() == 0 && !global_state.scan_state->finished);
}

static void ReadGeoParquetPushdownComplexFilter(ClientContext &context, duckdb::LogicalGet &get,
                                                duckdb::FunctionData *bind_data_p,
                                                vector<duckdb::unique_ptr<duckdb::Expression>> &filters) {
    auto &bind_data = bind_data_p->Cast<ReadGeoParquetBindData>();
    if (filters.empty()) {
        return;
    }

    duckdb::FilterCombiner combiner(context);
    for (auto &expr : filters) {
        combiner.AddFilter(expr->Copy());
    }

    vector<duckdb::FilterPushdownResult> pushdown_results;
    auto table_filters = combiner.GenerateTableScanFilters(get.GetColumnIds(), pushdown_results);

    for (auto &entry : table_filters.filters) {
        bind_data.pushed_filters.filters[entry.first] = std::move(entry.second);
    }
}

static duckdb::unique_ptr<duckdb::NodeStatistics> ReadGeoParquetCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ReadGeoParquetBindData>();
	return duckdb::make_uniq<duckdb::NodeStatistics>(bind_data.initial_file_cardinality);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto geoparquet_scanner_scalar_function =
	    ScalarFunction("geoparquet_scanner", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GeoparquetScannerScalarFun);

	loader.RegisterFunction(geoparquet_scanner_scalar_function);

	// Register another scalar function
	auto geoparquet_scanner_openssl_version_scalar_function = ScalarFunction("geoparquet_scanner_openssl_version", {LogicalType::VARCHAR},
	                                                             LogicalType::VARCHAR, GeoparquetScannerOpenSSLVersionScalarFun);
	loader.RegisterFunction(geoparquet_scanner_openssl_version_scalar_function);

	// Register the table function
	TableFunction read_geoparquet("read_geoparquet", {LogicalType::VARCHAR}, ReadGeoParquetScan, ReadGeoParquetBind, ReadGeoParquetInitGlobal);
	read_geoparquet.projection_pushdown = true;
	read_geoparquet.filter_pushdown = false;
	read_geoparquet.filter_prune = false;
	read_geoparquet.pushdown_complex_filter = ReadGeoParquetPushdownComplexFilter;
	read_geoparquet.cardinality = ReadGeoParquetCardinality;
	loader.RegisterFunction(read_geoparquet);
}

void GeoparquetScannerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string GeoparquetScannerExtension::Name() {
	return "geoparquet_scanner";
}

std::string GeoparquetScannerExtension::Version() const {
#ifdef EXT_VERSION_GEOPARQUET_SCANNER
	return EXT_VERSION_GEOPARQUET_SCANNER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(geoparquet_scanner, loader) {
	duckdb::LoadInternal(loader);
}
}
