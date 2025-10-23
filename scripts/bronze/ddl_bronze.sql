
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
  batch_start_time timestamptz;
  batch_end_time   timestamptz;
  start_time       timestamptz;
  end_time         timestamptz;


  v_cust_info_path   text := 'cust_info.csv';
  v_prd_info_path    text := 'prd_info.csv';
  v_sales_details_path text := 'sales_details.csv';
  v_loc_a101_path    text := 'loc_a101.csv';
  v_cust_az12_path   text := 'cust_az12.csv';
  v_px_cat_g1v2_path text := 'px_cat_g1v2.csv';
BEGIN
  batch_start_time := clock_timestamp();

  RAISE NOTICE '================================================';
  RAISE NOTICE 'Loading Bronze Layer';
  RAISE NOTICE '================================================';

  RAISE NOTICE '------------------------------------------------';
  RAISE NOTICE 'Loading CRM Tables';
  RAISE NOTICE '------------------------------------------------';

  -- crm_cust_info
  start_time := clock_timestamp();
  RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
  TRUNCATE TABLE bronze.crm_cust_info;

  RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
  EXECUTE format(
    $$COPY bronze.crm_cust_info
       FROM %L
       WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')$$,
    v_cust_info_path
  );
  end_time := clock_timestamp();
  RAISE NOTICE '>> Load Duration: % seconds',
    round(extract(epoch from (end_time - start_time))::numeric, 3);
  RAISE NOTICE '>> -------------';

  -- crm_prd_info
  start_time := clock_timestamp();
  RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
  TRUNCATE TABLE bronze.crm_prd_info;

  RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
  EXECUTE format(
    $$COPY bronze.crm_prd_info
       FROM %L
       WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')$$,
    v_prd_info_path
  );
  end_time := clock_timestamp();
  RAISE NOTICE '>> Load Duration: % seconds',
    round(extract(epoch from (end_time - start_time))::numeric, 3);
  RAISE NOTICE '>> -------------';

  -- crm_sales_details  (dates may be 'YYYYMMDD' or '0' in your CSV)
  -- NOTE: your current bronze table defines these three as TEXT, so raw load is fine.
  -- If you want to turn '0' into NULL on load, set NULL '0' below.
  start_time := clock_timestamp();
  RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
  TRUNCATE TABLE bronze.crm_sales_details;

  RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
  EXECUTE format(
    $$COPY bronze.crm_sales_details
       FROM %L
       WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '0')$$,
    v_sales_details_path
  );
  end_time := clock_timestamp();
  RAISE NOTICE '>> Load Duration: % seconds',
    round(extract(epoch from (end_time - start_time))::numeric, 3);
  RAISE NOTICE '>> -------------';

  RAISE NOTICE '------------------------------------------------';
  RAISE NOTICE 'Loading ERP Tables';
  RAISE NOTICE '------------------------------------------------';

  -- erp_loc_a101
  start_time := clock_timestamp();
  RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
  TRUNCATE TABLE bronze.erp_loc_a101;

  RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
  EXECUTE format(
    $$COPY bronze.erp_loc_a101
       FROM %L
       WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')$$,
    v_loc_a101_path
  );
  end_time := clock_timestamp();
  RAISE NOTICE '>> Load Duration: % seconds',
    round(extract(epoch from (end_time - start_time))::numeric, 3);
  RAISE NOTICE '>> -------------';

  -- erp_cust_az12
  start_time := clock_timestamp();
  RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
  TRUNCATE TABLE bronze.erp_cust_az12;

  RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
  EXECUTE format(
    $$COPY bronze.erp_cust_az12
       FROM %L
       WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')$$,
    v_cust_az12_path
  );
  end_time := clock_timestamp();
  RAISE NOTICE '>> Load Duration: % seconds',
    round(extract(epoch from (end_time - start_time))::numeric, 3);
  RAISE NOTICE '>> -------------';

  -- erp_px_cat_g1v2
  start_time := clock_timestamp();
  RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
  TRUNCATE TABLE bronze.erp_px_cat_g1v2;

  RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
  EXECUTE format(
    $$COPY bronze.erp_px_cat_g1v2
       FROM %L
       WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')$$,
    v_px_cat_g1v2_path
  );
  end_time := clock_timestamp();
  RAISE NOTICE '>> Load Duration: % seconds',
    round(extract(epoch from (end_time - start_time))::numeric, 3);
  RAISE NOTICE '>> -------------';

  batch_end_time := clock_timestamp();
  RAISE NOTICE '==========================================';
  RAISE NOTICE 'Loading Bronze Layer is Completed';
  RAISE NOTICE '   - Total Load Duration: % seconds',
    round(extract(epoch from (batch_end_time - batch_start_time))::numeric, 3);
  RAISE NOTICE '==========================================';

EXCEPTION
  WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
    RAISE NOTICE '==========================================';
    RAISE; 
END;
$$;
