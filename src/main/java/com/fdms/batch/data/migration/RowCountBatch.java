package com.fdms.batch.data.migration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import com.fdms.batch.data.migration.vo.DMScript;

@Component("RowCountBatch")
public class RowCountBatch {

	@Autowired
	protected JdbcTemplate jdbcTemplate;
	
	private static final Logger logger = LoggerFactory.getLogger(RowCountBatch.class);
	
	public HashMap<String,String> commonTablesRowCount(){
		
		HashMap<String, String> commonTablesmap = new HashMap<String,String>();
		
		String groupConcatMaxLenQuery = "SET group_concat_max_len = 1048576;";
		
		String rowCountQuery = "SELECT GROUP_CONCAT(CONCAT('SELECT ',QUOTE(db),' table_schema,',QUOTE(tb)," +
				"' table_name,COUNT(1) table_rows FROM `',db,'`.`',tb,'`') SEPARATOR ' UNION ')" +
				"INTO @CountSQL" +
				" FROM (SELECT table_schema db,table_name tb FROM information_schema.tables WHERE " +
				"table_schema IN ('fdms_commondata','fdmsus_share','reportmetrics','webfdmscommondata','webfdmsusers','webfdmsusers_keystone') ) A;";
		
		String prepareQuery = "PREPARE s FROM @CountSQL;";
		String executeQuery = "EXECUTE s; ";
		String deallocateQuery = "DEALLOCATE PREPARE s;";
		
		try{
			jdbcTemplate.execute(groupConcatMaxLenQuery);
			
			jdbcTemplate.execute(rowCountQuery);
			
			jdbcTemplate.execute(prepareQuery);
			
			jdbcTemplate.query(executeQuery,
					new ResultSetExtractor<HashMap<String,String>>() {
						@Override
						public HashMap<String,String> extractData(ResultSet rs)
								throws SQLException, DataAccessException {

							
							logger.info("TALE SCHEMA" + "\t" + "TABLE NAME" + "\t" + "TOTAL ROW COUNT");
							while (rs.next()) {							
								String tableSchema = rs.getString("table_schema");
								String tableName = rs.getString("table_name");
								String tableRows = rs.getString("table_rows");
							
								logger.info(tableSchema + "," + tableName + "," + tableRows);
								commonTablesmap.put(tableSchema+"."+tableName, tableRows);
							}
							return commonTablesmap;
						}
					});
			
			jdbcTemplate.execute(deallocateQuery);
			
		}
		catch(Exception e){
			
			logger.info("Exception  inside commonTablesRowCount method : " + e.getMessage());
		}
		return commonTablesmap;
	}
	
	public HashMap<String, String> keystoneSpecificRowCount(){
		
		HashMap<String, String> keystoneTablesmap = new HashMap<String,String>();
		
		String fetchAllTablesInGivenSchemaQuery = "SELECT table_schema,table_name FROM information_schema.tables" +
					" WHERE table_schema IN ('fdms_commondata','fdmsus_share','reportmetrics','webfdmscommondata','webfdmsusers','webfdmsusers_keystone')";
		try{
		jdbcTemplate.query(fetchAllTablesInGivenSchemaQuery,
				new ResultSetExtractor<List<DMScript>>() {
					@Override
					public List<DMScript> extractData(ResultSet rs)
							throws SQLException, DataAccessException {

						List<DMScript> list = new ArrayList<DMScript>();
						
						//logger.info("TALE SCHEMA" + "\t" + "TABLE NAME");
						while (rs.next()) {
							String tableSchema = rs.getString("table_schema");
							String tableName = rs.getString("table_name");
							//logger.info(tableSchema + "." + tableName);
							keystoneTablesmap.put(tableSchema+"."+tableName,tableSchema+"."+tableName);
						}
						return list;
					}
				});
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return keystoneTablesmap;	
	}
	
	
	@SuppressWarnings("deprecation")
	public void getRowCountByCompanyId(){
		
		HashMap<String,String> map = keystoneSpecificRowCount();
		
		List<String> companyIdList = new ArrayList<String>();
		
		companyIdList.add(map.get("fdms_commondata.CallCounts"));
		
		if(map.containsKey("webfdmsusers.companyoptionvalues")){
			companyIdList.add(map.get("webfdmsusers.companyoptionvalues"));
		}
		else{
			companyIdList.add(map.get("webfdmsusers_keystone.companyoptionvalues"));
		}
		
		if(map.containsKey("webfdmsusers.usersecurity")){
			companyIdList.add(map.get("webfdmsusers.usersecurity"));
		}
		else{
			companyIdList.add(map.get("webfdmsusers_keystone.usersecurity"));
		}
		
		if(map.containsKey("webfdmsusers.companies")){
			companyIdList.add(map.get("webfdmsusers.companies"));
		}
		else{
			companyIdList.add(map.get("webfdmsusers_keystone.companies"));
		}
		
		companyIdList.add(map.get("fdmsus_share.gb_entries"));
		companyIdList.add(map.get("fdms_commondata.smsscheduling"));
		companyIdList.add(map.get("fdmsus_share.uploaded_files"));
		companyIdList.add(map.get("fdmsus_share.eregister"));
		
		if(map.containsKey("webfdmsusers.locationoptionvalues")){
			companyIdList.add(map.get("webfdmsusers.locationoptionvalues"));
		}
		else{
			companyIdList.add(map.get("webfdmsusers_keystone.locationoptionvalues"));
		}
		
		companyIdList.add(map.get("webfdmscommondata.smsscheduling"));
		companyIdList.add(map.get("fdms_commondata.ClientLog"));
		
		String schemaTable = null;
		if(companyIdList.size() > 0){
			logger.info("===================================BASED ON COMPANY ID============================================");
			for(int i = 0; i < companyIdList.size(); i++){
				schemaTable = companyIdList.get(i);
				try{
					if(!schemaTable.equals("fdmsus_share.gb_entries"))
					{
						if(map.containsKey("webfdmsusers.companies")){
							
							String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
									 " WHERE CompanyID IN " +
									 "(SELECT CompanyID FROM webfdmsusers.companies WHERE UPPER(NAME) = 'KEYSTONE');";
					
							int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
							
							
							String splittedStr = splitString(schemaTable);
							logger.info(splittedStr + keystoneRowCount);
						}
						else if(map.containsKey("webfdmsusers_keystone.companies")){
							String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
											 " WHERE CompanyID IN " +
											 "(SELECT CompanyID FROM webfdmsusers_keystone.companies WHERE UPPER(NAME) = 'KEYSTONE');";
							
							int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
							
							String splittedStr = splitString(schemaTable);
							logger.info(splittedStr + keystoneRowCount);
							//logger.info(schemaTable + "\t" + keystoneRowCount);
						}
					}
					else
					{
						if(map.containsKey("webfdmsusers.companies"))
						{
							String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
									 " WHERE company_id IN " +
									 "(SELECT CompanyID FROM webfdmsusers.companies WHERE UPPER(NAME) = 'KEYSTONE');";
					
							int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
							
							String splittedStr = splitString(schemaTable);
							logger.info(splittedStr + keystoneRowCount);
						}
						else if(map.containsKey("webfdmsusers_keystone.companies")){
							String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
									 " WHERE company_id IN " +
									 "(SELECT CompanyID FROM webfdmsusers_keystone.companies WHERE UPPER(NAME) = 'KEYSTONE');";
					
							int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
							
							String splittedStr = splitString(schemaTable);
							logger.info(splittedStr + keystoneRowCount);
						}
							//logger.info(schemaTable + "\t" + keystoneRowCount);
					}
				}
				catch(Exception e){	
					e.printStackTrace();
				}
			}
		}
	}
	
	public void getRowCountByUserId(){
		
		HashMap<String,String> map = keystoneSpecificRowCount();
		
		List<String> userIdList = new ArrayList<String>();
		
		if(map.containsKey("webfdmsusers.userlog")){
			userIdList.add(map.get("webfdmsusers.userlog"));
		}
		else{
			userIdList.add(map.get("webfdmsusers_keystone.userlog"));
		}
		
		if(map.containsKey("webfdmsusers.ums_rolesmembership")){
			userIdList.add(map.get("webfdmsusers.ums_rolesmembership"));
		}
		else{
			userIdList.add(map.get("webfdmsusers_keystone.ums_rolesmembership"));
		}
		
		if(map.containsKey("webfdmsusers.userlocations")){
			userIdList.add(map.get("webfdmsusers.userlocations"));
		}
		else{
			userIdList.add(map.get("webfdmsusers_keystone.userlocations"));
		}
		
		userIdList.add(map.get("reportmetrics.transactionlog"));
		
		if(map.containsKey("webfdmsusers.duplicateloginlog")){
			userIdList.add(map.get("webfdmsusers.duplicateloginlog"));
		}
		else{
			userIdList.add(map.get("webfdmsusers_keystone.duplicateloginlog"));
		}
		
		if(map.containsKey("webfdmsusers.actionclasslog")){
			userIdList.add(map.get("webfdmsusers.actionclasslog"));
		}
		else{
			userIdList.add(map.get("webfdmsusers_keystone.actionclasslog"));
		}
		
		if(map.containsKey("webfdmsusers.uservacations")){
			userIdList.add(map.get("webfdmsusers.uservacations"));
		}
		else{
			userIdList.add(map.get("webfdmsusers_keystone.uservacations"));
		}
		
		String schemaTable = null;
		if(userIdList.size() > 0){
			logger.info("===================================BASED ON USER ID============================================");
			for(int i = 0; i < userIdList.size(); i++){
				schemaTable = userIdList.get(i);
				try{
					if(!(schemaTable.equals("webfdmsusers.userlog") || schemaTable.equals("webfdmsusers.userlocations") ||
						schemaTable.equals("webfdmsusers_keystone.userlog") || schemaTable.equals("webfdmsusers_keystone.userlocations") ))
					{
						if(map.containsKey("webfdmsusers.usersecurity")){
							String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
											 " WHERE userid IN " +
											 "(SELECT userid FROM webfdmsusers.usersecurity WHERE companyid IN(SELECT companyid FROM webfdmsusers.companies WHERE UPPER(NAME) = 'KEYSTONE'));";
							
							int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
							
							String splittedStr = splitString(schemaTable);
							logger.info(splittedStr + keystoneRowCount);
						}
						else if(map.containsKey("webfdmsusers_keystone.usersecurity")){
							String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
									 " WHERE userid IN " +
									 "(SELECT userid FROM webfdmsusers_keystone.usersecurity WHERE companyid IN(SELECT companyid FROM webfdmsusers_keystone.companies WHERE UPPER(NAME) = 'KEYSTONE'));";
					
							int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
							
							String splittedStr = splitString(schemaTable);
							logger.info(splittedStr + keystoneRowCount);
						}
					}
					else
					{
						if(map.containsKey("webfdmsusers.usersecurity")){
							String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
									 " WHERE user_id IN " +
									 "(SELECT userid FROM webfdmsusers.usersecurity WHERE companyid IN(SELECT companyid FROM webfdmsusers.companies WHERE UPPER(NAME) = 'KEYSTONE'));";
					
							int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
							
							String splittedStr = splitString(schemaTable);
							logger.info(splittedStr + keystoneRowCount);
						}
						else if(map.containsKey("webfdmsusers_keystone.usersecurity")){
							String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
									 " WHERE user_id IN " +
									 "(SELECT userid FROM webfdmsusers_keystone.usersecurity WHERE companyid IN(SELECT companyid FROM webfdmsusers_keystone.companies WHERE UPPER(NAME) = 'KEYSTONE'));";
					
							int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
							
							String splittedStr = splitString(schemaTable);
							logger.info(splittedStr + keystoneRowCount);
						}
						//logger.info(schemaTable + "\t" + keystoneRowCount);
					}
				}
				catch(Exception e){	
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public void getRowCountByClientDBName(){
		
		HashMap<String,String> map = keystoneSpecificRowCount();
		
		List<String> clientDbList = new ArrayList<String>();
		
		clientDbList.add(map.get("fdms_commondata.TableAnalysis"));
		clientDbList.add(map.get("fdms_commondata.PaymentData0210_0211"));
		clientDbList.add(map.get("fdms_commondata.PaymentData"));
		clientDbList.add(map.get("fdms_commondata.TableListing"));
		
		String schemaTable = null;
		if(clientDbList.size() > 0){
			logger.info("===================================BASED ON CLIENT DATABASE NAME============================================");
			for(int i = 0; i < clientDbList.size(); i++){
				schemaTable = clientDbList.get(i);
				try{
					if(schemaTable.equals("fdms_commondata.TableAnalysis"))
					{
						String rcQuery = "SELECT COUNT(*) " +
										" FROM " + schemaTable  +
										" WHERE ClientName = 'fdmsus_295_keystone' OR " +
										" ClientName = 'fdmsus_keystone'";
						
						int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
						
						String splittedStr = splitString(schemaTable);
						logger.info(splittedStr + keystoneRowCount);
						//logger.info(schemaTable + "\t" + keystoneRowCount);
					}
					else if(schemaTable.equals("fdms_commondata.PaymentData0210_0211"))
					{
						String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
										" WHERE ClientDatabase = 'fdmsus_295_keystone' OR " +
										" ClientDatabase = 'fdmsus_keystone'";
				
						int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
						
						String splittedStr = splitString(schemaTable);
						logger.info(splittedStr + keystoneRowCount);
						//logger.info(schemaTable + "\t" + keystoneRowCount);
					}
					else if(schemaTable.equals("fdms_commondata.PaymentData"))
					{
						String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
										" WHERE ClientDatabase = 'fdmsus_295_keystone' OR " +
										" ClientDatabase = 'fdmsus_keystone'";
				
						int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
						
						String splittedStr = splitString(schemaTable);
						logger.info(splittedStr + keystoneRowCount);
						//logger.info(schemaTable + "\t" + keystoneRowCount);
					}
					else
					{
						String rcQuery = "SELECT COUNT(*) FROM " + schemaTable + 
										" WHERE TableName = 'fdmsus_295_keystone' OR " +
										" TableName = 'fdmsus_keystone'";
				
						int keystoneRowCount = jdbcTemplate.queryForInt(rcQuery);
						
						String splittedStr = splitString(schemaTable);
						logger.info(splittedStr + keystoneRowCount);
						//logger.info(schemaTable + "\t" + keystoneRowCount);
					}
					
				}
				catch(Exception e){	
					e.printStackTrace();
				}
			}
		}
	}
	
	public String splitString(String str){
		StringBuffer sb = new StringBuffer();
		for (String retval: str.split("\\."))
		{
	         sb.append(retval);
	         sb.append(",");
	    }
		return sb.toString();
	}
	
}
