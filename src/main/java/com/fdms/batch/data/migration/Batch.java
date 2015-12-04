package com.fdms.batch.data.migration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import com.fdms.batch.data.migration.vo.DMScript;

@Component("Batch")
public class Batch {

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	@Value("${batch.run.id}")
	private String runId;
	
	private static final Logger logger = LoggerFactory.getLogger(Batch.class);

	public void processData() {
		String query = "SELECT  s.script_id, s.script,s.query_type,l.order_no,s.order_no_within_group "
				+ " FROM fdms_data_migration_script s, fdms_data_migration_table_list l "
				+ " WHERE s.table_schema = l.table_schema AND  s.table_name = l.table_name "
				+ " ORDER BY  l.order_no, s.order_no_within_group;";
		try {
			List<DMScript> scriptList = jdbcTemplate.query(query,
					new ResultSetExtractor<List<DMScript>>() {
						@Override
						public List<DMScript> extractData(ResultSet rs)
								throws SQLException, DataAccessException {

							List<DMScript> list = new ArrayList<DMScript>();
							while (rs.next()) {
								DMScript dms = new DMScript();
								dms.setScript(rs.getString("script"));
								dms.setOrderNo(rs.getInt("order_no"));
								dms.setOrderNoGroup(rs
										.getInt("order_no_within_group"));
								dms.setScriptId(rs.getInt("script_id"));
								dms.setQueryType(rs.getString("query_type"));
								list.add(dms);
							}
							return list;
						}
					});

			if (scriptList != null && !scriptList.isEmpty()) {
				for (int i = 0; i < scriptList.size(); i++) {
					String type = null;
					String script = null;
					Integer scriptId = null;
					try {
						DMScript dataMigrationScript = scriptList.get(i);
						type = dataMigrationScript.getQueryType();
						script = dataMigrationScript.getScript();
						scriptId = dataMigrationScript.getScriptId();
						if ("DDL".equalsIgnoreCase(type)) {
							jdbcTemplate.execute(script);
							
							insertLog("processed DDL", scriptId, runId, 0);
							logger.info("processed DDL for ScriptId : "+scriptId +" Execution Status : " +0);
						} else {
							insertLog("Not processed", scriptId, runId, 1);
							logger.info("Not processed for ScriptId : "+scriptId +" Execution Status : " +1);
						}
					} catch (Exception e) {
						System.out.println("Failed id = " + scriptId + " | "
								+ e.getMessage());
						insertLog("Not processed Error=" + e.getMessage(),
								scriptId, runId, 1);
						logger.error("Not processed Error : " + e.getMessage()+
								" ScriptId :" + scriptId + " Execution Status : "+ 1);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void insertLog(String log, int scriptId, String runid, int status) {
		StringBuilder sbSql = new StringBuilder();
		sbSql.append("insert into fdms_data_migration_log(log,script_id,run_id,status) ");
		sbSql.append("values('").append(log).append("',").append(scriptId)
				.append(",'").append(runid).append("',").append(status)
				.append(");");

		jdbcTemplate.update(sbSql.toString());
	}

}
