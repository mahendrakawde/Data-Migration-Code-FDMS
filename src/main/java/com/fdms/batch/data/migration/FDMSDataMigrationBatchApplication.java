package com.fdms.batch.data.migration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class FDMSDataMigrationBatchApplication {
	public static void main(String[] args) {
	
		
		ConfigurableApplicationContext confAppContext = SpringApplication.run(
				FDMSDataMigrationBatchApplication.class, args);

		RowCountBatch rowCountBatch = confAppContext.getBean("RowCountBatch", RowCountBatch.class);
		
		rowCountBatch.commonTablesRowCount();		
		rowCountBatch.getRowCountByCompanyId();
		rowCountBatch.getRowCountByUserId();
		rowCountBatch.getRowCountByClientDBName();
	}
}
