EXEC silver.load_silver 

CREATE OR ALTER PROCEDURE silver.load_silver AS
 BEGIN

	--start by checking for nulls or duplicates in the primary key
	  -- 1. rank the duplicates and selects only the most recent create date 
	  -- 2. trim all unwanted spaces
	  -- 3. Change the short forms to full 

	TRUNCATE TABLE silver.crm_cust_info;
	insert into silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

	select cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case when Upper(trim(cst_marital_status))= 'M' THEN 'MARRIED'
		 when upper(trim(cst_marital_status)) = 'S' then 'SINGLE'
		 else 'n/a'
	end cst_marital_status,  -- Normalize marital status values to readable format
	case when Upper(trim(cst_gndr)) = 'F' THEN 'FEMALE'
		 when upper(trim(cst_gndr)) = 'M' then 'MALE'
		 else 'n/a'
	end cst_gndr,  -- Normalize gender values to readable format
	cst_create_date
	from (
	Select *, 
	Row_Number() OVER (Partition by cst_id order by cst_create_date Desc) as flag_last
	from [DataWarehouse].[bronze].[crm_cust_info]
	WHERE cst_id IS NOT NULL
	)t where flag_last = 1 -- Select the most recent record per customer


	--separate the prd_key into 2- cat_id(first 5 characters) and prd_key
	--replace the dash in the cat_id to underscore
	--cast. to change the data format to date
	TRUNCATE TABLE silver.crm_prd_info;
	insert into [silver].[crm_prd_info](
		   [prd_id],
		  [prd_key],
		  [cat_id],
		  [prd_nm],
		  [prd_cost],
		  [prd_line],
		  [prd_start_dt],
		  [prd_end_dt]
	)
	SELECT [prd_id],
		 substring(prd_key, 7, len(prd_key)) as prd_key, -- Extract product key
		  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
		  [prd_nm],
		  ISNULL ([prd_cost], 0) as prd_cost,
		  CASE WHEN Upper(trim(prd_line)) = 'M' then 'Mountain'
				WHEN Upper(trim(prd_line)) = 'R' then 'Road'
				WHEN Upper(trim(prd_line)) = 'S' then 'Other Sales'
				WHEN Upper(trim(prd_line)) = 'T' then 'Touring'
				else 'n/a'
		  end [prd_line],
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)- 1 AS DATE
		) AS prd_end_dt -- Calculate end date as one day before the next start date
	  FROM [DataWarehouse].[bronze].[crm_prd_info]


	  --convert all zeros in the date to nulls
	  -- cast the int to varchar then date. you cant convert directly from int to date
	 TRUNCATE TABLE [silver].[crm_sales_details]
	 INSERT INTO [silver].[crm_sales_details] (
		[sls_ord_num],
		  [sls_prd_key],
		  [sls_cust_id],
		  [sls_order_dt],
		 [sls_ship_dt],
		  [sls_due_dt],
		  [sls_sales],
		  [sls_quantity],
		 [sls_price]
	)
	SELECT [sls_ord_num],
		  [sls_prd_key],
		  [sls_cust_id],
		 CASE WHEN sls_order_dt = 0 or len(sls_order_dt) != 8 THEN NULL 
		  ELSE CAST(CAST(sls_order_dt as varchar) as date)
		 end as [sls_order_dt],
		 CASE WHEN sls_ship_dt = 0 or len(sls_ship_dt) != 8 THEN NULL 
		  ELSE CAST(CAST(sls_ship_dt as varchar) as date)
		 end as [sls_ship_dt],
		CASE WHEN sls_due_dt = 0 or len(sls_due_dt) != 8 THEN NULL 
		  ELSE CAST(CAST(sls_due_dt as varchar) as date)
		 end as [sls_due_dt],
		  [sls_quantity],
		  case when sls_sales is null or sls_sales < = 0 or sls_sales != sls_quantity * ABS(sls_price)
			 then sls_quantity * ABS(sls_price)
			 else sls_sales
			end as sls_sales, --Recalculate sales if original value is missing or incorrect
		  case when sls_price is null or sls_price <= 0 then sls_sales / nullif (sls_quantity, 0)
		  else sls_price
		  end as sls_price-- derive price is original value is invalid
	  FROM [DataWarehouse].[bronze].[crm_sales_details]

	  --

	-- 
	truncate table [silver].[erp_cust_az12]
	insert into [silver].[erp_cust_az12] (
		cid,
		bdate,
		gen 
	)
	select
	case when cid like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	else cid
	end cid,--remove 'NAS' prefix is present
	case when bdate > GETDATE() THEN NULL
			ELSE bdate
	end as bdate, -- set furture birthdates to null
	CASE WHEN gen is null or gen = ' ' then 'n/a' 
	 when gen = 'M' then 'Male'
	 when gen = 'F' then 'Female'
	 else gen --Normalize gender values and handle unknown cases
	 --Ayooluwa's method
	end as gen
	 /* Case when upper(trim(gen)) in ('F', 'Female') then 'Female'
			when upper(trim(gen)) in ('M', 'Male') then 'Male'
			else 'n/a'
		end as gen2 --Baraa's method */
	from [bronze].[erp_cust_az12]

	truncate table [silver].[erp_loc_a101]
	insert into [silver].[erp_loc_a101] (
			cid, cntry)
	select 
	replace (cid, '-','') cid,
	case when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('US', 'USA') then 'United States'
		when Trim(cntry) = '' or cntry is null then 'n/a'
		else trim(cntry)
	end cntry
	from [bronze].[erp_loc_a101]



	truncate table [silver].[erp_px_cat_g1v2]
	insert into [silver].[erp_px_cat_g1v2] (
		id,
		cat,
		subcat,
		maintenance)
	select 
		id,
		cat,
		subcat,
		maintenance
	from [bronze].[erp_px_cat_g1v2]
END
