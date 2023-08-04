package com.aliyun.dts.deliver.commons.etl.impl;

import com.aliyun.dts.deliver.commons.etl.impl.WhiteList.DB_CATEGORY;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DeprecatedWhiteList {
	public static String jsonStr;
	public static ObjectMapper mapper;
	public static Map<String, Map<String, Object>> dbData;
	public static Set<String> databases = new HashSet<String>();
	public static Set<String> lowerCaseDatabases = new HashSet<String>();
	public static Set<String> trimCaseDatabases = new HashSet<String>();


	public static Map<String, Set<String>> db_schema_dest = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_table = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> lower_case_db_table = new HashMap<String, Set<String>>();


	public static Map<String, Set<String>> db_view = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_synonym = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_procedure = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_function = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_type = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_rule = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_default = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_plan = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_package = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_sequence = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_domain = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_aggregate = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_operator = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_extension = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> db_job = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> table_col = new HashMap<String, Set<String>>();
	public static Map<String, Set<String>> lower_case_table_col = new HashMap<String, Set<String>>();


	public static Map<String, String> db_tablegroup = new HashMap<String, String>();
	public static Map<String, String> db_message_key = new HashMap<String, String>();

	public static Map<String, String> table_filter = new HashMap<String, String>();
	public static Map<String, Object> table_filter_mongo = new HashMap<String, Object>();
	public static Map<String, String> table_family = new HashMap<String, String>();
	public static Map<String, String> conditions = new HashMap<String, String>();
	public static Map<String, String> dts_mapper = new HashMap<String, String>();
	public static Map<String, String> view_mapper = new HashMap<String, String>();
	public static Map<String, String> synonym_mapper = new HashMap<String, String>();
	public static Map<String, String> procedure_mapper = new HashMap<String, String>();
	public static Map<String, String> function_mapper = new HashMap<String, String>();
	public static Map<String, String> type_mapper = new HashMap<String, String>();
	public static Map<String, String> rule_mapper = new HashMap<String, String>();
	public static Map<String, String> default_mapper = new HashMap<String, String>();
	public static Map<String, String> plan_mapper = new HashMap<String, String>();
	public static Map<String, String> package_mapper = new HashMap<String, String>();
	public static Map<String, String> sequence_mapper = new HashMap<String, String>();
	public static Map<String, String> domain_mapper = new HashMap<String, String>();
	public static Map<String, String> aggregate_mapper = new HashMap<String, String>();
	public static Map<String, String> operator_mapper = new HashMap<String, String>();
	public static Map<String, String> extension_mapper = new HashMap<String, String>();
	public static Map<String, String> job_mapper = new HashMap<String, String>();

	public static Map<String, Boolean> has_all = new HashMap<String, Boolean>();

	public static Map<String, Boolean> lowercase_has_all = new HashMap<String, Boolean>();

	//db case sen
	public static Map<String, Boolean> case_sen = new HashMap<String, Boolean>();


	//Drds
	public static Map<String, String> table_part_db_key = new HashMap<String, String>();
	public static Map<String, String> table_part_tb_key = new HashMap<String, String>();
	public static Map<String, String> tab_num_eachdb = new HashMap<String, String>();

	//ads
	public static Map<String, String> table_type = new HashMap<String, String>();
	public static Map<String, String> table_primary_key = new HashMap<String, String>();
	public static Map<String, String> table_cluster = new HashMap<String, String>();
	public static Map<String, String> table_part_key = new HashMap<String, String>();
	public static Map<String, String> table_part_num = new HashMap<String, String>();
	public static Map<String, String> table_db_name = new HashMap<String, String>();

	// Tablestore
	public static Map<String, String> table_col_tablestore_target_type = new HashMap<>();

	//
	public static Map<String, String> db_dml_operations = new HashMap<String, String>();

	public static Map<String, String> table_dml_operations = new HashMap<String, String>();

	public static Map<String, String> db_ddl_operations = new HashMap<String, String>();

	public static Map<String, String> table_ddl_operations = new HashMap<String, String>();

	//odps log table
	public static Map<String, String> table_partitions = new HashMap<String, String>();

	//conflict
	public static Map<String, String> db_conflict = new HashMap<String, String>();

	public static Map<String, String> tab_conflict = new HashMap<String, String>();


	public static Boolean initialized = false;


	public static Map<String, Set<String>> attach_table_col = new HashMap<String, Set<String>>();


	public static Map<String, String> table_col_type = new HashMap<String, String>();

	public static Map<String, String> attach_table_col_type = new HashMap<String, String>();

	public static Map<String, String> table_col_default_value = new HashMap<String, String>();

	public static Map<String, String> attach_table_col_value = new HashMap<String, String>();

	public static Map<String, Boolean> table_col_attach = new HashMap<String, Boolean>();

	public static Map<String, String> table_col_value = new HashMap<String, String>();


	//es
	public static Map<String, Integer> db_shard = new HashMap<String, Integer>();

	public static Map<String, Integer> table_shard = new HashMap<String, Integer>();

	public static Map<String, Integer> db_replica = new HashMap<String, Integer>();


	public static Map<String, String> db_analysis = new HashMap<String, String>();

	public static Map<String, String> db_analyzer = new HashMap<String, String>();

	public static Map<String, String> db_timezone = new HashMap<String, String>();

	public static Map<String, Boolean> table_is_partition = new HashMap<String, Boolean>();

	public static Map<String, String> table_partition_key = new HashMap<String, String>();

	public static Map<String, String> table_id = new HashMap<String, String>();

	public static Map<String, String> table_id_value = new HashMap<String, String>();

	public static Map<String, Boolean> table_is_join = new HashMap<String, Boolean>();

	public static Map<String, String> table_relation_role = new HashMap<String, String>();

	public static Map<String, String> table_parent_name = new HashMap<String, String>();

	public static Map<String, String> table_parent_id = new HashMap<String, String>();

	public static Map<String, String> table_index = new HashMap<String, String>();

	public static Map<String, Boolean> table_col_index_value = new HashMap<String, Boolean>();

	public static Map<String, String> table_col_analyzer = new HashMap<String, String>();

	public static Map<String, String> db_index_mapping = new HashMap<String, String>();

	public static Map<String, String> table_message_key = new HashMap<String, String>();

	public static Map<String, Set<String>> topic_partion_keys = new HashMap<String, Set<String>>();

	public static Map<String, String> db_table_pattern_type = new HashMap<String, String>();


	private static DB_CATEGORY srcCategory = WhiteList.DB_CATEGORY.NORMAL;

	/*
	 * DBs DB下所有schema schema下的所有表。 DB 映射 ** table 映射 column 映射
	 */
	public static void init(String json) {
		init(json, DB_CATEGORY.NORMAL);
	}

	public static void init(String json, DB_CATEGORY db_category) {
		init(json, db_category, db_category);
	}

	public static void init(String json, DB_CATEGORY src_category, DB_CATEGORY dest_category) {
		synchronized (initialized) {
			if (initialized)
				return;

			srcCategory = src_category;
			databases = new HashSet<String>();
			db_schema_dest = new HashMap<String, Set<String>>();
			db_table = new HashMap<String, Set<String>>();
			db_view = new HashMap<String, Set<String>>();
			db_synonym = new HashMap<String, Set<String>>();
			db_procedure = new HashMap<String, Set<String>>();
			db_function = new HashMap<String, Set<String>>();
			db_type = new HashMap<String, Set<String>>();
			db_rule = new HashMap<String, Set<String>>();
			db_default = new HashMap<String, Set<String>>();
			db_plan = new HashMap<String, Set<String>>();
			db_package = new HashMap<String, Set<String>>();
			db_sequence = new HashMap<String, Set<String>>();
			db_domain = new HashMap<String, Set<String>>();
			db_aggregate = new HashMap<String, Set<String>>();

			db_operator = new HashMap<String, Set<String>>();
			db_extension = new HashMap<String, Set<String>>();

			db_message_key = new HashMap<String, String>();

			table_col = new HashMap<String, Set<String>>();
			table_filter = new HashMap<String, String>();
			table_filter_mongo = new HashMap<String, Object>();
			conditions = new HashMap<String, String>();
			dts_mapper = new HashMap<String, String>();
			view_mapper = new HashMap<String, String>();
			synonym_mapper = new HashMap<String, String>();
			procedure_mapper = new HashMap<String, String>();
			function_mapper = new HashMap<String, String>();
			type_mapper = new HashMap<String, String>();
			rule_mapper = new HashMap<String, String>();
			default_mapper = new HashMap<String, String>();
			plan_mapper = new HashMap<String, String>();
			package_mapper = new HashMap<String, String>();
			sequence_mapper = new HashMap<String, String>();
			domain_mapper = new HashMap<String, String>();
			aggregate_mapper = new HashMap<String, String>();
			operator_mapper = new HashMap<String, String>();
			extension_mapper = new HashMap<String, String>();
			job_mapper = new HashMap<String, String>();
			has_all = new HashMap<String, Boolean>();
			case_sen = new HashMap<String, Boolean>();

			table_part_db_key = new HashMap<String, String>();
			table_part_tb_key = new HashMap<String, String>();
			tab_num_eachdb = new HashMap<String, String>();

			table_type = new HashMap<String, String>();
			table_primary_key = new HashMap<String, String>();
			table_cluster = new HashMap<String, String>();
			table_part_key = new HashMap<String, String>();
			table_part_num = new HashMap<String, String>();
			table_db_name = new HashMap<String, String>();

			db_dml_operations = new HashMap<String, String>();

			table_dml_operations = new HashMap<String, String>();

			db_ddl_operations = new HashMap<String, String>();

			table_ddl_operations = new HashMap<String, String>();

			table_partitions = new HashMap<String, String>();

			initialized = true;
		}

		Map<String, Map<String, Object>> dbData;
		try {
			String db_dest = null;
			String schema_src = null;
			String schema_dest = null;
			String table_src = null;
			String table_dest = null;
			boolean is_all;

			ObjectMapper mapper = new ObjectMapper(); // can reuse, share
			// globally
			dbData = mapper.readValue(json, Map.class);
			Set<String> dbSet = dbData.keySet();
			for (String dbName_tmp : dbSet) {// 遍历所有库
				String dbName = alterName(dbName_tmp, src_category);
				databases.add(dbName);
				Map<String, Object> itemMap = dbData.get(dbName_tmp);
				Set<String> itemSet = itemMap.keySet();
				if (itemMap.containsKey("name")) {
					db_dest = (String) itemMap.get("name");
					db_dest = alterName(db_dest, dest_category);
					dts_mapper.put(dbName, db_dest);
				}
				if (itemMap.containsKey("tableGroup")) {
					String tablegroup = (String) itemMap.get("tableGroup");
					db_tablegroup.put(dbName, tablegroup);
				}
				if (itemMap.containsKey("all")) {
					boolean isTabAll = (Boolean) itemMap.get("all");
					has_all.put(dbName, isTabAll);
					lowercase_has_all.put(dbName.toLowerCase(), isTabAll);
				}

				if (itemMap.containsKey("case_sen")) {
					boolean isCaseSen = (Boolean) itemMap.get("case_sen");
					case_sen.put(dbName, isCaseSen);
				}

				if (itemMap.containsKey("dml_op")) {
					String key = (String) itemMap.get("dml_op");
					db_dml_operations.put(dbName, key);
				}

				if (itemMap.containsKey("ddl_op")) {
					String key = (String) itemMap.get("ddl_op");
					db_ddl_operations.put(dbName, key);
				}


				if (itemMap.containsKey("conflict")) {
					String key = (String) itemMap.get("conflict");
					db_conflict.put(dbName, key);
				}

				//es
				if (itemMap.containsKey("shard")) {
					Integer key = (Integer) itemMap.get("shard");
					db_shard.put(dbName, key);
				}

				if (itemMap.containsKey("replica")) {
					Integer key = (Integer) itemMap.get("replica");
					db_replica.put(dbName, key);
				}


				if (itemMap.containsKey("analysis")) {
					String key = (String) itemMap.get("analysis");
					db_analysis.put(dbName, key);
				}

				if (itemMap.containsKey("analyzer")) {
					String key = (String) itemMap.get("analyzer");
					db_analyzer.put(dbName, key);
				}

				if (itemMap.containsKey("time_zone")) {
					String key = (String) itemMap.get("time_zone");
					db_timezone.put(dbName, key);
				}

				//es
				if (itemMap.containsKey("index_mapping")) {
					String key = (String) itemMap.get("index_mapping");
					db_index_mapping.put(dbName, key);
				}

				//kafka
				if (itemMap.containsKey("message_key")) {
					String key = (String) itemMap.get("message_key");
					db_message_key.put(dbName, key);
				}

				for (String itemName : itemSet) {// 遍历每个库中的参数
					if (itemName.equals("Table")) {// 表的信息
						Map<String, Object> tableMap = (Map<String, Object>) itemMap.get("Table");
						Set<String> tableNameSet = tableMap.keySet();

						is_all = false;
						schema_dest = null;
						schema_src = null;
						table_src = null;
						for (String tableName_tmp : tableNameSet) {// 遍历该库下所有的表
							String tableName = alterName(tableName_tmp, src_category);
							Map<String, Object> tableItemMap = (Map<String, Object>) tableMap.get(tableName_tmp);
							Set<String> tableItemSet = tableItemMap.keySet();
							table_src = tableName;
							if (tableItemMap.containsKey("srcSchema")) {
								String srcSchema = (String) tableItemMap.get("srcSchema");
								schema_src = alterName(srcSchema, src_category);
								table_src = schema_src + "." + tableName;
							}
							if (tableItemMap.containsKey("destSchema")) {
								String destSchema = (String) tableItemMap.get("destSchema");
								schema_dest = alterName(destSchema, dest_category);
								if (db_schema_dest.containsKey(db_dest)) {
									Set<String> tmp_set = db_schema_dest.get(db_dest);
									tmp_set.add(schema_dest);
									db_schema_dest.put(db_dest, tmp_set);
								} else {
									Set<String> tmp_set = new HashSet<String>();
									tmp_set.add(schema_dest);
									db_schema_dest.put(db_dest, tmp_set);
								}
							}
							if (tableItemMap.containsKey("name")) {// 映射关系
								String targetTabName = (String) tableItemMap.get("name");
								table_dest = alterName(targetTabName, dest_category);
								String tmp_src = dbName + "." + table_src;
								String tmp_dest;
								if (schema_dest == null)
									tmp_dest = table_dest;
								else
									tmp_dest = schema_dest + "." + table_dest;
								dts_mapper.put(tmp_src, tmp_dest);

							}
							if (tableItemMap.containsKey("all")) {// 全部包含
								boolean isColAll = (Boolean) tableItemMap.get("all");
								String tmp = dbName + "." + table_src;
								has_all.put(tmp, isColAll);
								lowercase_has_all.put(tmp.toLowerCase(), isColAll);
							}
							if (tableItemMap.containsKey("filter")) {// 映射关系

								if (src_category == DB_CATEGORY.MONGO) {
									Object filter = tableItemMap.get("filter");
									String tmp = dbName + "." + table_src;
									table_filter_mongo.put(tmp, filter);
								} else {
									String filter = (String) tableItemMap.get("filter");
									String tmp = dbName + "." + table_src;
									table_filter.put(tmp, filter);
								}
							}

							if (tableItemMap.containsKey("part_db_key")) {//part_db_key
								String key = (String) tableItemMap.get("part_db_key");
								String tmp = dbName + "." + table_src;
								table_part_db_key.put(tmp, key);
							}

							if (tableItemMap.containsKey("part_tab_key")) { //part_tab_key
								String key = (String) tableItemMap.get("part_tab_key");
								String tmp = dbName + "." + table_src;
								table_part_tb_key.put(tmp, key);
							}

							if (tableItemMap.containsKey("tab_num_eachdb")) { //tab_num_eachdb
								String key = tableItemMap.get("tab_num_eachdb").toString();
								String tmp = dbName + "." + table_src;
								tab_num_eachdb.put(tmp, key);
							}

							if (tableItemMap.containsKey("type")) {
								String key = tableItemMap.get("type").toString();
								String tmp = dbName + "." + table_src;
								table_type.put(tmp, key);
							}

							if (tableItemMap.containsKey("primary_key")) {
								String key = tableItemMap.get("primary_key").toString();
								String tmp = dbName + "." + table_src;
								table_primary_key.put(tmp, key);
							}

							if (tableItemMap.containsKey("cluster")) {
								String key = tableItemMap.get("cluster").toString();
								String tmp = dbName + "." + table_src;
								table_cluster.put(tmp, key);
							}

							if (tableItemMap.containsKey("part_key")) {
								String key = tableItemMap.get("part_key").toString();
								String tmp = dbName + "." + table_src;
								table_part_key.put(tmp, key);
							}

							if (tableItemMap.containsKey("part_num")) {
								String key = tableItemMap.get("part_num").toString();
								String tmp = dbName + "." + table_src;
								table_part_num.put(tmp, key);
							}

							if (tableItemMap.containsKey("dbName")) {
								String key = tableItemMap.get("dbName").toString();
								String tmp = dbName + "." + table_src;
								table_db_name.put(tmp, key);
							}


							if (tableItemMap.containsKey("family")) {// Family
								String family = (String) tableItemMap.get("family");
								String tmp = dbName + "." + table_src;
								table_family.put(tmp, family);
							}

							if (tableItemMap.containsKey("dml_op")) {//part_db_key
								String key = (String) tableItemMap.get("dml_op");
								String tmp = dbName + "." + table_src;
								table_dml_operations.put(tmp, key);
							}

							if (tableItemMap.containsKey("ddl_op")) {//part_db_key
								String key = (String) tableItemMap.get("ddl_op");
								String tmp = dbName + "." + table_src;
								table_ddl_operations.put(tmp, key);
							}

							if (tableItemMap.containsKey("partition")) {//part_db_key
								String key = (String) tableItemMap.get("partition");
								String tmp = dbName + "." + table_src;
								table_partitions.put(tmp, key);
							}

							if (tableItemMap.containsKey("conflict")) {//part_db_key
								String key = (String) tableItemMap.get("conflict");
								String tmp = dbName + "." + table_src;
								tab_conflict.put(tmp, key);
							}

							//es
							if (tableItemMap.containsKey("shard")) {//
								Integer key = (Integer) tableItemMap.get("shard");
								String tmp = dbName + "." + table_src;
								table_shard.put(tmp, key);
							}


							if (tableItemMap.containsKey("is_partition")) {//
								Boolean key = (Boolean) tableItemMap.get("is_partition");
								String tmp = dbName + "." + table_src;
								table_is_partition.put(tmp, key);
							}
							if (tableItemMap.containsKey("partition_key")) {//
								String key = (String) tableItemMap.get("partition_key");
								String tmp = dbName + "." + table_src;
								table_partition_key.put(tmp, key);
							}
							if (tableItemMap.containsKey("shard")) {//
								Integer key = (Integer) tableItemMap.get("shard");
								String tmp = dbName + "." + table_src;
								table_shard.put(tmp, key);
							}
							if (tableItemMap.containsKey("_id")) {//
								String key = (String) tableItemMap.get("_id");
								String tmp = dbName + "." + table_src;
								table_id.put(tmp, key);
							}
							if (tableItemMap.containsKey("_idvalue")) {//
								String key = (String) tableItemMap.get("_idvalue");
								String tmp = dbName + "." + table_src;
								table_id_value.put(tmp, key);
							}

							if (tableItemMap.containsKey("is_join")) {//
								Boolean key = (Boolean) tableItemMap.get("is_join");
								String tmp = dbName + "." + table_src;
								table_is_join.put(tmp, key);
							}
							if (tableItemMap.containsKey("relation_role")) {//
								String key = (String) tableItemMap.get("relation_role");
								String tmp = dbName + "." + table_src;
								table_relation_role.put(tmp, key);
							}
							if (tableItemMap.containsKey("parent_name")) {//
								String key = (String) tableItemMap.get("parent_name");
								String tmp = dbName + "." + table_src;
								table_parent_name.put(tmp, key);
							}
							if (tableItemMap.containsKey("parent_id")) {//
								String key = (String) tableItemMap.get("parent_id");
								String tmp = dbName + "." + table_src;
								table_parent_id.put(tmp, key);
							}
							if (tableItemMap.containsKey("index")) {//
								String key = (String) tableItemMap.get("index");
								String tmp = dbName + "." + table_src;
								table_index.put(tmp, key);
							}

							if (tableItemMap.containsKey("message_key")) {//
								String key = (String) tableItemMap.get("message_key");
								String tmp = dbName + "." + table_src;
								table_message_key.put(tmp, key);
							}

							if (tableItemMap.containsKey("patternType")) {//
								String key = (String) tableItemMap.get("patternType");
								String tmp = dbName + "." + table_src;
								db_table_pattern_type.put(tmp, key);
							}

							for (String tableItemName : tableItemSet) {// 遍历该表下所有的属性
								if (tableItemName.equals("column")) {// 映射关系
									if (tableItemMap.get("column") != null) {
										Map<String, Object> colMap = (Map<String, Object>) tableItemMap.get("column");
										Set<String> colSet = colMap.keySet();
										for (String colName_tmp : colSet) {
											String colName = alterName(colName_tmp, src_category);
											String tmp_name;
											if (schema_src == null) {
												tmp_name = dbName + "." + tableName;
											} else {
												tmp_name = dbName + "." + schema_src + "." + tableName;
											}
											if (table_col.containsKey(tmp_name)) {
												Set<String> tmp_set = table_col.get(tmp_name);
												tmp_set.add(colName);
												table_col.put(tmp_name, tmp_set);
											} else {
												Set<String> tmp_set = new HashSet<String>();
												tmp_set.add(colName);
												table_col.put(tmp_name, tmp_set);
											}

											if (!(colMap.get(colName_tmp) instanceof Map)) {
												System.out.println("fuckkkk");

											}
											Map<String, Object> colItemMap = (Map<String, Object>) colMap.get(colName_tmp);
											String colTargetName = (String) colItemMap.get("name");
											String colType = (String) colItemMap.get("type");
											String colDefaultValue = (String) colItemMap.get("defaultValue");
											Boolean colIndexValue = (Boolean) colItemMap.get("index_value");
											String colAnalyzer = (String) colItemMap.get("analyzer");
											Boolean colPrimaryKey = (Boolean) colItemMap.get("primaryKey");
											String targetType = (String) colItemMap.get("targetType");
											String colValue = (String) colItemMap.get("value");


											Boolean sharedKey = (Boolean) colItemMap.get("sharedKey");


											boolean isAttachColumn = false;
											Object object = colItemMap.get("attachColumn");
											if (object != null)
												isAttachColumn = (Boolean) object;

											colTargetName = alterName(colTargetName, dest_category);
											if (schema_src == null) {
												tmp_name = dbName + "." + tableName + "." + colName;
											} else {
												tmp_name = dbName + "." + schema_src + "." + tableName + "." + colName;
											}
											dts_mapper.put(tmp_name, colTargetName);
											table_col_type.put(tmp_name, colType);
											table_col_default_value.put(tmp_name, colDefaultValue);
											table_col_attach.put(tmp_name, isAttachColumn);

											table_col_index_value.put(tmp_name, colIndexValue);
											table_col_analyzer.put(tmp_name, colAnalyzer);
											table_col_tablestore_target_type.put(tmp_name, targetType);
											table_col_value.put(tmp_name, colValue);


											if (null != sharedKey && sharedKey) {
												String tmp_table = dbName + "." + tableName;
												Set<String> keys = topic_partion_keys.get(tmp_table);
												if (null == keys) {
													keys = new HashSet<String>();
													topic_partion_keys.put(tmp_table, keys);
												}
												keys.add(colName);
											}
										}
									}

								}

								//增加的列
								if (tableItemName.equals("attach_column")) {// 映射关系
									if (tableItemMap.get("attach_column") != null) {
										Map<String, Object> colMap = (Map<String, Object>) tableItemMap.get("attach_column");
										Set<String> colSet = colMap.keySet();
										for (String colName_tmp : colSet) {
											String colName = alterName(colName_tmp, src_category);
											String tmp_name;
											if (schema_src == null) {
												tmp_name = dbName + "." + tableName;
											} else {
												tmp_name = dbName + "." + schema_src + "." + tableName;
											}
											if (attach_table_col.containsKey(tmp_name)) {
												Set<String> tmp_set = attach_table_col.get(tmp_name);
												tmp_set.add(colName);
												attach_table_col.put(tmp_name, tmp_set);
											} else {
												Set<String> tmp_set = new HashSet<String>();
												tmp_set.add(colName);
												attach_table_col.put(tmp_name, tmp_set);
											}

											Map<String, Object> colItemMap = (Map<String, Object>) colMap.get(colName_tmp);
											//name
											String colTargetName = (String) colItemMap.get("name");
											colTargetName = alterName(colTargetName, dest_category);
											if (schema_src == null) {
												tmp_name = dbName + "." + tableName + "." + colName;
											} else {
												tmp_name = dbName + "." + schema_src + "." + tableName + "." + colName;
											}
											dts_mapper.put(tmp_name, colTargetName);

											//type
											String type = (String) colItemMap.get("type");
											if (schema_src == null) {
												tmp_name = dbName + "." + tableName + "." + colName;
											} else {
												tmp_name = dbName + "." + schema_src + "." + tableName + "." + colName;
											}
											attach_table_col_type.put(tmp_name, type);

											//value
											String value = (String) colItemMap.get("value");
											if (schema_src == null) {
												tmp_name = dbName + "." + tableName + "." + colName;
											} else {
												tmp_name = dbName + "." + schema_src + "." + tableName + "." + colName;
											}
											attach_table_col_value.put(tmp_name, value);
										}
									}

								}
							}
							if (db_table.containsKey(dbName)) {
								Set<String> tmp_set = db_table.get(dbName);
								tmp_set.add(table_src);
								db_table.put(dbName, tmp_set);
							} else {
								Set<String> tmp_set = new HashSet<String>();
								tmp_set.add(table_src);
								db_table.put(dbName, tmp_set);
							}
						}
					} else if (itemName.equals("View")) {
						elementStructure(itemMap, dbName, "View", view_mapper, db_view, src_category);
					} else if (itemName.equals("Synonym")) {
						elementStructure(itemMap, dbName, "Synonym", synonym_mapper, db_synonym, src_category);
					} else if (itemName.equals("Procedure")) {
						elementStructure(itemMap, dbName, "Procedure", procedure_mapper, db_procedure, src_category);
					} else if (itemName.equals("Function")) {
						elementStructure(itemMap, dbName, "Function", function_mapper, db_function, src_category);
					} else if (itemName.equals("Type")) {
						elementStructure(itemMap, dbName, "Type", type_mapper, db_type, src_category);
					} else if (itemName.equals("Rule")) {
						elementStructure(itemMap, dbName, "Rule", rule_mapper, db_rule, src_category);
					} else if (itemName.equals("Default")) {
						elementStructure(itemMap, dbName, "Default", default_mapper, db_default, src_category);
					} else if (itemName.equals("Plan")) {
						elementStructure(itemMap, dbName, "Plan", plan_mapper, db_plan, src_category);
					} else if (itemName.equals("Package")) {
						elementStructure(itemMap, dbName, "Package", package_mapper, db_package, src_category);
					} else if (itemName.equals("Sequence")) {
						elementStructure(itemMap, dbName, "Sequence", sequence_mapper, db_sequence, src_category);
					} else if (itemName.equals("Domain")) {
						elementStructure(itemMap, dbName, "Domain", domain_mapper, db_domain, src_category);
					} else if (itemName.equals("Aggregate")) {
						elementStructure(itemMap, dbName, "Aggregate", aggregate_mapper, db_aggregate, src_category);
					} else if (itemName.equals("Operator")) {
						elementStructure(itemMap, dbName, "Operator", operator_mapper, db_operator, src_category);
					} else if (itemName.equals("Extension")) {
						elementStructure(itemMap, dbName, "Extension", extension_mapper, db_extension, src_category);
					} else if (itemName.equals("Job")) {
						elementStructure(itemMap, dbName, "Job", job_mapper, db_job, src_category);
					}
				}
			}
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String alterName(String name, DB_CATEGORY category) {
		String ret = name;
		if (category.equals(DB_CATEGORY.MSSQL)) {
			ret = "[" + ret + "]";
		}
		return ret;
	}

	public static void elementStructure(Map<String, Object> itemMap, String dbName, String element_type, Map<String, String> mapper,
                                        Map<String, Set<String>> db_element, DB_CATEGORY db_category) {
		Map<String, Object> tableMap = (Map<String, Object>) itemMap.get(element_type);
		Set<String> tableNameSet = tableMap.keySet();

		String schema_dest = null;
		String schema_src = null;
		String table_src = null;
		for (String tableName_tmp : tableNameSet) {// 遍历该库下所有的表
			String tableName = alterName(tableName_tmp, db_category);
			Map<String, Object> tableItemMap = (Map<String, Object>) tableMap.get(tableName_tmp);
			Set<String> tableItemSet = tableItemMap.keySet();
			table_src = alterName(tableName, db_category);
			if (tableItemMap.containsKey("srcSchema")) {
				String srcSchema = (String) tableItemMap.get("srcSchema");
				schema_src = alterName(srcSchema, db_category);
				table_src = schema_src + "." + tableName;
			}
			if (tableItemMap.containsKey("destSchema")) {
				String destSchema = (String) tableItemMap.get("destSchema");
				schema_dest = alterName(destSchema, db_category);
			}
			if (tableItemMap.containsKey("name")) {// 映射关系
				String targetTabName = (String) tableItemMap.get("name");
				String tmp_src = dbName + "." + table_src;
				String tmp_dest = alterName(targetTabName, db_category);
				if (schema_dest == null)
					tmp_dest = tmp_dest;
				else
					tmp_dest = schema_dest + "." + tmp_dest;
				mapper.put(tmp_src, tmp_dest);
			}
			if (db_element.containsKey(dbName)) {
				Set<String> tmp_set = db_element.get(dbName);
				String tmp;
				if (schema_src == null)
					tmp = tableName;
				else
					tmp = schema_src + "." + tableName;
				tmp_set.add(tmp);
				db_element.put(dbName, tmp_set);
			} else {
				Set<String> tmp_set = new HashSet<String>();
				String tmp;
				if (schema_dest == null)
					tmp = tableName;
				else
					tmp = schema_src + "." + tableName;
				tmp_set.add(tmp);
				db_element.put(dbName, tmp_set);
			}
		}
	}

	public static Set<String> allDatabases() {
//		for(String database:databases){
//			trimCaseDatabases.add(database.trim());
//		}
//		return trimCaseDatabases;
		return databases;
	}

	public static Set<String> allLowerCaseDatabases() {
//		if(lowerCaseDatabases.size()==0){
//			for(String database:databases){
//				lowerCaseDatabases.add(database.toLowerCase());
//			}
//		}

		for (String database : databases) {
			lowerCaseDatabases.add(database.toLowerCase());
		}

		return lowerCaseDatabases;
	}

	public static Set<String> getTables(String database) {
		if (db_table.containsKey(database)) {
			return db_table.get(database);
		} else
			return null;
	}

	public static Set<String> getLowerCaseDbTables(String database) {
		if (!lower_case_db_table.containsKey(database)) {
			Set<String> tables = db_table.get(database);
			if (tables != null && !tables.isEmpty()) {
				Set<String> lowerCaseTables = new HashSet<String>();

				for (String table : tables) {
					lowerCaseTables.add(table.toLowerCase());
				}
				lower_case_db_table.put(database, lowerCaseTables);
			}


		}

		if (lower_case_db_table.containsKey(database)) {
			return lower_case_db_table.get(database);
		} else
			return null;

	}


	public static Set<String> getDestSchemas(String database) {
		if (db_schema_dest.containsKey(database))
			return db_schema_dest.get(database);
		else
			return null;
	}

	public static String getTablegroup(String database) {
		if (db_tablegroup.containsKey(database))
			return db_tablegroup.get(database);
		return null;
	}

	public static String getFilter(String database, String table) {
		String tmp = database + "." + table;
		if (table_filter.containsKey(tmp))
			return table_filter.get(tmp);
		return null;
	}

	public static Object getFilter(String database, String table, DB_CATEGORY category) {
		String tmp = database + "." + table;
		if (category == DB_CATEGORY.MONGO) {
			if (table_filter_mongo.containsKey(tmp))
				return table_filter_mongo.get(tmp);
		} else {
			if (table_filter.containsKey(tmp))
				return (String) table_filter.get(tmp);
		}
		return null;
	}

	public static String getFamily(String database, String table) {
		String tmp = database + "." + table;
		if (table_family.containsKey(tmp))
			return table_family.get(tmp);
		return null;
	}

	public static Set<String> getColumns(String table) {
		if (table_col.containsKey(table))
			return table_col.get(table);
		return null;
	}

	public static Set<String> getColumns(String database, String table) {
		String tmp = database + "." + table;
		if (table_col.containsKey(tmp))
			return table_col.get(tmp);
		return null;
	}

	public static Set<String> getLowerCaseColumns(String database, String table) {
		String tmp = database + "." + table;

		if (!lower_case_table_col.containsKey(tmp)) {
			Set<String> columns = table_col.get(tmp);

			if (columns != null && !columns.isEmpty()) {
				Set<String> lowerCaseColumns = new HashSet<String>();

				for (String column : columns) {
					lowerCaseColumns.add(column.toLowerCase());
				}
				lower_case_table_col.put(tmp, lowerCaseColumns);
			}

		}

		if (lower_case_table_col.containsKey(tmp))
			return lower_case_table_col.get(tmp);
		return null;
	}

	public static Set<String> getAttachColumns(String table) {
		if (attach_table_col.containsKey(table))
			return attach_table_col.get(table);
		return null;
	}

	public static Set<String> getAttachColumns(String database, String table) {
		String tmp = database + "." + table;
		if (attach_table_col.containsKey(tmp))
			return attach_table_col.get(tmp);
		return null;
	}

	public static String getColumnType(String database, String table, String column) {
		String tmp = database + "." + table + "." + column;
		return table_col_type.get(tmp);

	}

	public static String getAttachColumnType(String database, String table, String column) {
		String tmp = database + "." + table + "." + column;
		return attach_table_col_type.get(tmp);

	}

	public static String getColumnDefaultValue(String database, String table, String column) {
		String tmp = database + "." + table + "." + column;
		return table_col_default_value.get(tmp);

	}

	public static String getAttachColumnValue(String database, String table, String column) {
		String tmp = database + "." + table + "." + column;
		return attach_table_col_value.get(tmp);

	}

	public static String getColumnValue(String database, String table, String column) {
		String tmp = database + "." + table + "." + column;
		return table_col_value.get(tmp);

	}

	public static boolean isAttachColumn(String database, String table, String column) {
		String tmp = database + "." + table + "." + column;
		return table_col_attach.get(tmp);

	}

	public static Boolean hasAll(String database) {
		if (has_all.containsKey(database))
			return has_all.get(database);
		return false;
	}

	public static Boolean hasLowerCaseAll(String database) {
		if (lowercase_has_all.containsKey(database))
			return lowercase_has_all.get(database);
		return false;

	}

	public static Boolean caseSen(String database) {
		if (case_sen.containsKey(database))
			return case_sen.get(database);
		return false;
	}

	public static Boolean hasAll(String database, String table) {
		if (has_all.containsKey(database) && (has_all.get(database)))
			return has_all.get(database);
		String tmp = database + "." + table;
		if (has_all.containsKey(tmp))
			return has_all.get(tmp);
		return false;
	}

	public static Boolean hasLowerCaseAll(String database, String table) {
		if (lowercase_has_all.containsKey(database) && (lowercase_has_all.get(database)))
			return lowercase_has_all.get(database);
		String tmp = database + "." + table;
		if (lowercase_has_all.containsKey(tmp))
			return lowercase_has_all.get(tmp);
		return false;
	}

	public static String dbMapper(String database) {
		if (dts_mapper.containsKey(database))
			return dts_mapper.get(database);
		return database;
	}

	public static String dbMapper(String database, String table) {
		String tmp = database + "." + table;
		if (dts_mapper.containsKey(tmp))
			return dts_mapper.get(tmp);

		if ((WhiteList.DB_CATEGORY.ELK.equals(srcCategory))) {
			String indexName = table;

			//转义
			indexName = indexName.replace("+", "_");
			indexName = indexName.replace("-", "_");
			indexName = indexName.replace("&&", "_");
			indexName = indexName.replace("||", "_");
			indexName = indexName.replace("!", "_");
			indexName = indexName.replace("()", "_");
			indexName = indexName.replace("{}", "_");
			indexName = indexName.replace("^", "_");
			indexName = indexName.replace("\"", "_");
			indexName = indexName.replace("~", "_");
			indexName = indexName.replace("*", "_");
			indexName = indexName.replace("?", "_");
			indexName = indexName.replace(":", "_");
			indexName = indexName.replace("\\", "_");
			indexName = indexName.replace("%", "_");


			if ("db_tb".equals(dbIndexMapping(database))) {
				return database + "_" + indexName;
			} else
				return indexName;
		}

		return table;
	}

	public static String dbMapper(String database, String table, String column) {
		String tmp = database + "." + table + "." + column;
		if (dts_mapper.containsKey(tmp))
			return dts_mapper.get(tmp);
		return column;
	}

	public static Set<String> getViews(String database) {
		if (db_view.containsKey(database))
			return db_view.get(database);
		else
			return null;
	}

	public static Set<String> getSynonym(String database) {
		if (db_synonym.containsKey(database))
			return db_synonym.get(database);
		else
			return null;
	}

	public static Set<String> getProcedure(String database) {
		if (db_procedure.containsKey(database))
			return db_procedure.get(database);
		else
			return null;
	}

	public static Set<String> getFunction(String database) {
		if (db_function.containsKey(database))
			return db_function.get(database);
		else
			return null;
	}

	public static Set<String> getType(String database) {
		if (db_type.containsKey(database))
			return db_type.get(database);
		else
			return null;
	}

	public static Set<String> getRule(String database) {
		if (db_rule.containsKey(database))
			return db_rule.get(database);
		else
			return null;
	}

	public static Set<String> getDefault(String database) {
		if (db_default.containsKey(database))
			return db_default.get(database);
		else
			return null;
	}

	public static Set<String> getPlan(String database) {
		if (db_plan.containsKey(database))
			return db_plan.get(database);
		else
			return null;
	}

	public static Set<String> getPackage(String database) {
		if (db_package.containsKey(database))
			return db_package.get(database);
		else
			return null;
	}

	public static Set<String> getSequence(String database) {
		if (db_sequence.containsKey(database))
			return db_sequence.get(database);
		else
			return null;
	}

	public static Set<String> getDomain(String database) {
		if (db_domain.containsKey(database))
			return db_domain.get(database);
		else
			return null;
	}

	public static Set<String> getAggregate(String database) {
		if (db_aggregate.containsKey(database))
			return db_aggregate.get(database);
		else
			return null;
	}

	public static Set<String> getOperator(String database) {
		if (db_operator.containsKey(database))
			return db_operator.get(database);
		else
			return null;
	}

	public static Set<String> getExtension(String database) {
		if (db_extension.containsKey(database))
			return db_extension.get(database);
		else
			return null;
	}

	public static Set<String> getJob(String database) {
		if (db_job.containsKey(database))
			return db_job.get(database);
		else
			return null;
	}

	public static String dbDmlOperations(String database) {
		if (db_dml_operations.containsKey(database))
			return db_dml_operations.get(database);
		else
			return null;
	}

	public static String dbDdlOperations(String database) {
		if (db_ddl_operations.containsKey(database))
			return db_ddl_operations.get(database);
		else
			return null;
	}


	public static String viewMapper(String database, String table) {
		String tmp = database + "." + table;
		if (view_mapper.containsKey(tmp))
			return view_mapper.get(tmp);
		return table;
	}

	public static String synonymMapper(String database, String table) {
		String tmp = database + "." + table;
		if (synonym_mapper.containsKey(tmp))
			return synonym_mapper.get(tmp);
		return table;
	}

	public static String procedureMapper(String database, String table) {
		String tmp = database + "." + table;
		if (procedure_mapper.containsKey(tmp))
			return procedure_mapper.get(tmp);
		return table;
	}

	public static String functionMapper(String database, String table) {
		String tmp = database + "." + table;
		if (function_mapper.containsKey(tmp))
			return function_mapper.get(tmp);
		return table;
	}

	public static String typeMapper(String database, String table) {
		String tmp = database + "." + table;
		if (type_mapper.containsKey(tmp))
			return type_mapper.get(tmp);
		return table;
	}

	public static String ruleMapper(String database, String table) {
		String tmp = database + "." + table;
		if (rule_mapper.containsKey(tmp))
			return rule_mapper.get(tmp);
		return table;
	}

	public static String defaultMapper(String database, String table) {
		String tmp = database + "." + table;
		if (default_mapper.containsKey(tmp)) {
			return default_mapper.get(tmp);
		}
		return table;
	}

	public static String planMapper(String database, String table) {
		String tmp = database + "." + table;
		if (plan_mapper.containsKey(tmp)) {
			return plan_mapper.get(tmp);
		}
		return table;
	}

	public static String packageMapper(String database, String table) {
		String tmp = database + "." + table;
		if (package_mapper.containsKey(tmp)) {
			return package_mapper.get(tmp);
		}
		return table;
	}

	public static String sequenceMapper(String database, String table) {
		String tmp = database + "." + table;
		if (sequence_mapper.containsKey(tmp)) {
			return sequence_mapper.get(tmp);
		}
		return table;
	}

	public static String domainMapper(String database, String table) {
		String tmp = database + "." + table;
		if (domain_mapper.containsKey(tmp)) {
			return domain_mapper.get(tmp);
		}
		return table;
	}

	public static String aggregateMapper(String database, String table) {
		String tmp = database + "." + table;
		if (aggregate_mapper.containsKey(tmp)) {
			return aggregate_mapper.get(tmp);
		}
		return table;
	}

	public static String operatorMapper(String database, String table) {
		String tmp = database + "." + table;
		if (operator_mapper.containsKey(tmp)) {
			return operator_mapper.get(tmp);
		}
		return table;
	}

	public static String extensionMapper(String database, String table) {
		String tmp = database + "." + table;
		if (extension_mapper.containsKey(tmp)) {
			return extension_mapper.get(tmp);
		}
		return table;
	}

	public static String dbPartionKey(String database, String table) {
		String tmp = database + "." + table;
		if (table_part_db_key.containsKey(tmp)) {
			return table_part_db_key.get(tmp);
		}
		return null;
	}

	public static String tabPartionKey(String database, String table) {
		String tmp = database + "." + table;
		if (table_part_tb_key.containsKey(tmp)) {
			return table_part_tb_key.get(tmp);
		}
		return null;
	}

	public static String tabNumEachdb(String database, String table) {
		String tmp = database + "." + table;
		if (tab_num_eachdb.containsKey(tmp)) {
			return tab_num_eachdb.get(tmp);
		}
		return null;
	}

	public static String tableType(String database, String table) {
		String tmp = database + "." + table;
		if (table_type.containsKey(tmp)) {
			return table_type.get(tmp);
		}
		return null;
	}

	public static String tablePrimaryKey(String database, String table) {
		String tmp = database + "." + table;
		if (table_primary_key.containsKey(tmp)) {
			return table_primary_key.get(tmp);
		}
		return null;
	}

	public static String tableCluster(String database, String table) {
		String tmp = database + "." + table;
		if (table_cluster.containsKey(tmp)) {
			return table_cluster.get(tmp);
		}
		return null;
	}

	public static String tablePartKey(String database, String table) {
		String tmp = database + "." + table;
		if (table_part_key.containsKey(tmp)) {
			return table_part_key.get(tmp);
		}
		return null;
	}

	public static String tablePartNum(String database, String table) {
		String tmp = database + "." + table;
		if (table_part_num.containsKey(tmp)) {
			return table_part_num.get(tmp);
		}
		return null;
	}

	public static String tableDbName(String database, String table) {
		String tmp = database + "." + table;
		if (table_db_name.containsKey(tmp)) {
			return table_db_name.get(tmp);
		}
		return null;
	}

	public static String tableDmlOperations(String database, String table) {
		String tmp = database + "." + table;
		if (table_dml_operations.containsKey(tmp)) {
			return table_dml_operations.get(tmp);
		}
		return null;
	}

	public static String tableDdlOperations(String database, String table) {
		String tmp = database + "." + table;
		if (table_ddl_operations.containsKey(tmp)) {
			return table_ddl_operations.get(tmp);
		}
		return null;
	}

	public static String tablePartitions(String database, String table) {
		String tmp = database + "." + table;
		if (table_partitions.containsKey(tmp)) {
			return table_partitions.get(tmp);
		}
		return null;
	}

	public static String dbConfilct(String database) {
		if (db_conflict.containsKey(database)) {
			return db_conflict.get(database);
		}
		return null;
	}

	public static String tableConfilct(String database, String table) {
		String tmp = database + "." + table;
		if (tab_conflict.containsKey(tmp)) {
			return tab_conflict.get(tmp);
		}
		return null;
	}

	public static Integer dbShard(String database) {
		if (db_shard.containsKey(database)) {
			return db_shard.get(database);
		}
		return null;
	}

	public static Integer tableShard(String database, String table) {
		String tmp = database + "." + table;
		if (table_shard.containsKey(tmp)) {
			return table_shard.get(tmp);
		}
		return null;
	}

	public static Integer dbReplica(String database) {
		if (db_replica.containsKey(database)) {
			return db_replica.get(database);
		}
		return null;
	}

	public static String dbAnalysis(String database) {
		if (db_analysis.containsKey(database)) {
			return db_analysis.get(database);
		}
		return null;
	}

	public static String dbAnalyzer(String database) {
		if (db_analyzer.containsKey(database)) {
			return db_analyzer.get(database);
		}
		return null;
	}

	public static String dbTimezone(String database) {
		if (db_timezone.containsKey(database)) {
			return db_timezone.get(database);
		}
		return null;
	}

	public static Boolean tableIsPartition(String database, String table) {
		String tmp = database + "." + table;
		if (table_is_partition.containsKey(tmp)) {
			return table_is_partition.get(tmp);
		}
		return null;
	}

	public static String tablePartitionKey(String database, String table) {
		String tmp = database + "." + table;
		if (table_partition_key.containsKey(tmp)) {
			return table_partition_key.get(tmp);
		}
		return null;
	}

	public static String tableId(String database, String table) {
		String tmp = database + "." + table;
		if (table_id.containsKey(tmp)) {
			return table_id.get(tmp);
		}
		return null;
	}

	public static String tableIdValue(String database, String table) {
		String tmp = database + "." + table;
		if (table_id_value.containsKey(tmp)) {
			return table_id_value.get(tmp);
		}
		return null;
	}

	public static Boolean tableIsJoin(String database, String table) {
		String tmp = database + "." + table;
		if (table_is_join.containsKey(tmp)) {
			return table_is_join.get(tmp);
		}
		return null;
	}

	public static String tableRelationRole(String database, String table) {
		String tmp = database + "." + table;
		if (table_relation_role.containsKey(tmp)) {
			return table_relation_role.get(tmp);
		}
		return null;
	}

	public static String tableParentName(String database, String table) {
		String tmp = database + "." + table;
		if (table_parent_name.containsKey(tmp)) {
			return table_parent_name.get(tmp);
		}
		return null;
	}

	public static String tableParentId(String database, String table) {
		String tmp = database + "." + table;
		if (table_parent_id.containsKey(tmp)) {
			return table_parent_id.get(tmp);
		}
		return null;
	}

	public static String tableIndex(String database, String table) {
		String tmp = database + "." + table;
		if (table_index.containsKey(tmp)) {
			return table_index.get(tmp);
		}
		return null;
	}

	public static String dbMessageKey(String database) {
		if (db_message_key.containsKey(database)) {
			return db_message_key.get(database);
		}
		return null;
	}

	public static String tableMessageKey(String database, String table) {
		String tmp = database + "." + table;
		if (table_message_key.containsKey(tmp)) {
			return table_message_key.get(tmp);
		}
		return null;
	}

	public static Boolean getColumnIndexValue(String database, String table, String column) {
		String tmp = database + "." + table + "." + column;
		return table_col_index_value.get(tmp);
	}

	public static String getColumnAnalyzer(String database, String table, String column) {
		String tmp = database + "." + table + "." + column;
		return table_col_analyzer.get(tmp);
	}

	public static String dbIndexMapping(String database) {
		if (db_index_mapping.containsKey(database))
			return db_index_mapping.get(database);
		return null;
	}

	public static Set<String> getTopicPartitionKeys(String database, String table) {
		String tmp = database + "." + table;
		return topic_partion_keys.get(tmp);
	}

	public static String getColumnTargetType(String database, String table, String column) {
		String tmp = database + "." + table + "." + column;
		if (table_col_tablestore_target_type.containsKey(tmp))
			return table_col_tablestore_target_type.get(tmp);
		return null;
	}

	public static String getPatternType(String database, String table) {
		String tmp = database + "." + table;
		if (db_table_pattern_type.containsKey(tmp)) {
			return db_table_pattern_type.get(tmp);
		}
		return null;
	}
}
