CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
); 
INSERT INTO silver.crm_cust_info (
    cst_id,              
    cst_key,            
    cst_firstname,      
    cst_lastname,      
    cst_marital_status, 
    cst_gndr,       
    cst_create_date)
Select 
cst_id,cst_key,TRIM(cst_firstname)as cst_firstname,TRIM(cst_lastname)as cst_lastname,
CASE
	WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
	ELSE 'n/a'
END AS cst_marital_status,
CASE
	WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
	WHEN UPPER(TRIM(cst_gndr))='S' THEN 'Female'
	ELSE 'n/a'
END as cst_gndr,
cst_create_date
FROM(
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER by cst_create_date DESC) as flag
FROM bronze.crm_cust_info) temp
WHERE flag=1 AND cst_id is not null;

SELECT COUNT(*) FROM silver.crm_cust_info;


IF OBJECT_ID('silver.prd_info', 'U') IS NOT NULL
    DROP TABLE silver.prd_info
CREATE TABLE silver.prd_info(prd_id INT,cat_id NVARCHAR(50),prd_key NVARCHAR(50),prd_nm	NVARCHAR(50),prd_cost INT,prd_line NVARCHAR(50),prd_start_dt DATE,prd_end_dt DATE);
INSERT INTO silver.prd_info(prd_id,cat_id,
prd_key,
prd_nm	,prd_cost
,prd_line ,prd_start_dt ,prd_end_dt)

(
SELECT 
prd_id,
Replace(SUbstring(prd_key,1,5),'-','_')as cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) As prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'M' THEN 'Mountain'
	WHEN 'T' THEN 'Touring'
	Else 'n/a'
	END as prd_line,
CAST(prd_start_dt as DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER ( Partition by prd_key order by prd_start_dt)-1 As DATE)AS prd_end_dt
From bronze.crm_prd_info
)

SELECT* from silver.prd_info

CREATE TABLE silver.crm_sales_details(sls_ord_num NVARCHAR(50),sls_prd_key NVARCHAR(50),sls_cust_id INT,sls_order_dt DATE,sls_ship_dt DATE,sls_due_dt DATE,sls_sales INT,sls_quantity INT,sls_price INT);
INSERT INTO silver.crm_sales_details
(sls_ord_num ,sls_prd_key ,sls_cust_id ,sls_order_dt ,sls_ship_dt ,sls_due_dt ,sls_sales ,sls_quantity ,sls_price)
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price 
	END AS sls_price
FROM bronze.crm_sales_details;

CREATE TABLE silver.erp_cust_az12(cid NVARCHAR(50),bdate DATE,gen NVARCHAR(50))
INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, 
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen 
		FROM bronze.erp_cust_az12;
		SELECT * from silver.erp_cust_az12

CREATE TABLE silver.erp_loc_a101(cid NVARCHAR(50),cntry NVARCHAR(50))
INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
Bulk INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\saivi\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH(FIRSTROW=2,FIELDTERMINATOR=',',TABLOCK)
CREATE TABLE silver.erp_px_cat_g1v2(id NVARCHAR(50),cat NVARCHAR(50),subcat NVARCHAR(50),maintenance NVARCHAR(50))
INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
SELECT * FROM silver.erp_px_cat_g1v2
