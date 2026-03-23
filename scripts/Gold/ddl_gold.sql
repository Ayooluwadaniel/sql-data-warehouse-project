/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

create view gold.dim_customers AS -------create it into views
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname last_name,
	la.cntry as country,
	lower(ci.cst_marital_status) as marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' then lower(ci.cst_gndr) --CRM is master preference for gender
		ELSE lower(Coalesce(ca.gen, 'n/a'))
		end as Gender, --because the cst_gndr and gen rows were contradicting
	ca.bdate birthdate,
	ci.cst_create_date create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on	ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
create view gold.dim_products as

SELECT 
	 Row_Number() over(Order By pn.prd_start_dt, pn.prd_key) as product_key,
	  pn.[prd_id] product_id,
	  pn.[prd_key] product_number,
	  pn.[prd_nm] product_name,
      pn.[cat_id] category_id,
      pc.[cat] category,
	  pc.[subcat] subcategory,
	  pc.[maintenance],
      pn.[prd_cost] cost,
      pn.[prd_line] product_line,
      pn.[prd_start_dt] start_date
  FROM [silver].[crm_prd_info] as pn
  left join [silver].[erp_px_cat_g1v2] pc
  on pn.cat_id = pc.id
  where prd_end_dt is null --filters out all historical data
  GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

create view gold.fact_sales as
  SELECT sd.[sls_ord_num],
	  pr.product_key,
	  cu.customer_key,
      sd.[sls_prd_key],
      sd.[sls_cust_id],
      sd.[sls_order_dt],
      sd.[sls_ship_dt],
      sd.[sls_due_dt],
      sd.[sls_sales],
      sd.[sls_quantity],
      sd.[sls_price]
  FROM [silver].[crm_sales_details] as sd
  left join gold.dim_products pr
  on sd.sls_prd_key = pr.product_number
  left join gold.dim_customers as cu
  on sd.sls_cust_id = cu.customer_id
GO
