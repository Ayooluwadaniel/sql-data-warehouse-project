/* This stored procedure loads data into the 'bronze' schema
from the external csv files. 
It truncatrs the bronze tables before loading data.
it uses the bulk insert command to load data from csv files into the bronze tables
*/

EXEC bronze.load_bronze

Create or alter procedure bronze.load_bronze as 
Begin 
	Begin Try 
		PRINT '=============================='
		PRINT 'Loading Bronze Layer'
		PRINT '=============================='
		Truncate table [Bronze].[crm_cust_info];
		/*truncate deletes the content in the table first.
		before you now bulk insert.
		This helps to keep the table refreshed and up to date
		*/
		Bulk insert bronze.crm_cust_info
		from 'C:\Users\DELL\Documents\SQL Server Management Studio\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			Firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		Truncate Table bronze.crm_prd_info;
		Bulk insert bronze.crm_prd_info
		from 'C:\Users\DELL\Documents\SQL Server Management Studio\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		Truncate Table bronze.crm_sales_details;
		Bulk insert bronze.crm_sales_details
		from 'C:\Users\DELL\Documents\SQL Server Management Studio\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		Truncate Table bronze.erp_cust_az12;

		Bulk insert bronze.erp_cust_az12
		from 'C:\Users\DELL\Documents\SQL Server Management Studio\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		Truncate Table bronze.erp_loc_a101;
		Bulk insert bronze.erp_loc_a101
		from 'C:\Users\DELL\Documents\SQL Server Management Studio\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		Truncate table [Bronze].[erp_px_cat_g1v2]
		bulk insert [Bronze].[erp_px_cat_g1v2]
		from 'C:\Users\DELL\Documents\SQL Server Management Studio\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
	End try
	begin catch
		PRINT '======================'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error message:' + Error_Message();
		PRINT 'Error message:' + cast (error_number() as NVARCHAR);
		PRINT 'Error Message:' + cast (error_state() as NVARCHAR);
		PRINT '======================='
	end catch
End
