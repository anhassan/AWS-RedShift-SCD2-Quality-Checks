-- Dropping the table to delete the previous version
DROP TABLE IF EXISTS user_address_dim;


-- Creating the dimensional to store user addresses
CREATE TEMP TABLE user_address_dim (
    address VARCHAR(256),
    bus_eff_dt DATE,
    bus_exp_dt DATE
);


-- Populating the data for SCD 2 into user address dimension
INSERT INTO user_address_dim VALUES
('A','2022-11-03','2023-03-03'),
('A', '2023-01-01','2023-07-22'),
('A','2023-07-28','9999-12-31'),
('B', '2023-04-01','2023-08-01'),
('B', '2023-08-01','2024-01-31'),
('B', '2024-01-31','9999-12-31'),
('C', '2022-12-01','9999-12-31');


-- Querying the data in user address dimension
SELECT * FROM user_address_dim order by address,bus_eff_dt;


-- Finding the faulty SCD 2 addresses
with 
lag_dt_data as (
    select *,
        lag(bus_exp_dt,1) over(order by address,bus_eff_dt,bus_exp_dt) as lag_dt 
    from user_address_dim order by bus_eff_dt,bus_exp_dt
),
scd_diff_data as (
    select *
        ,case when datediff(day,bus_eff_dt,lag_dt) <> 0 then 1 else 0 end as scd_diff
    from lag_dt_data
    where lag_dt < '9999-12-31'
),
summarized_scd_diff_data as (
    select 
        address
        ,sum(scd_diff) as scd_mismatches
    from scd_diff_data
    group by address
)
select * from summarized_scd_diff_data 
where scd_mismatches > 0;


-- Creating Stored Procedure to automate faulty SCD 2 checks for all the tables
CREATE OR REPLACE PROCEDURE sp_check_table_scd_mismatches(table_name varchar(512), key_name varchar(512), temp_table INOUT varchar(256))
AS $$
BEGIN
   EXECUTE 'drop table if exists ' || temp_table;
   EXECUTE 'create temp table ' || temp_table || ' as
        with 
        lag_dt_data as (
            select *,
                lag(bus_exp_dt,1) over(order by ' || key_name || ',bus_eff_dt,bus_exp_dt) as lag_dt 
            from ' || table_name || ' order by bus_eff_dt,bus_exp_dt
        ),
        scd_diff_data as (
            select *
                ,case when datediff(day,bus_eff_dt,lag_dt) <> 0 then 1 else 0 end as scd_diff
            from lag_dt_data
            where lag_dt < ''9999-12-31''
        ),
        summarized_scd_diff_data as (
            select 
                ' || key_name || '
                ,sum(scd_diff) as scd_mismatches
            from scd_diff_data
            group by ' || key_name || '
        )
        select * from summarized_scd_diff_data 
        where scd_mismatches > 0;';
END;
$$ LANGUAGE plpgsql;


-- Calling the Stored Procedure with required parameters
CALL sp_check_table_scd_mismatches('user_address_dim','address','scd_diff_table');


-- Getting the results back
SELECT * FROM scd_diff_table;


-- Dropping the table to delete the previous version
DROP TABLE IF EXISTS user_contact_dim;


-- Creating the dimensional to store user contacts
CREATE TEMP TABLE user_contact_dim (
    contact VARCHAR(256),
    bus_eff_dt DATE,
    bus_exp_dt DATE
);


-- Populating the data for SCD 2 into user address dimension
INSERT INTO user_contact_dim VALUES
('A','2022-11-03','2023-03-03'),
('A', '2023-01-01','2023-07-22'),
('A','2023-07-28','9999-12-31'),
('B', '2023-04-01','2023-08-01'),
('B', '2023-08-11','2024-01-31'),
('B', '2024-01-31','9999-12-31'),
('C', '2022-12-01','9999-12-31');


-- Querying the data in user address dimension
SELECT * FROM user_contact_dim order by contact,bus_eff_dt;


-- Calling the SCD 2 Quality check Stored Procedure on both the tables with the required parameters
CALL sp_check_table_scd_mismatches('user_address_dim','address','scd_diff_address_table');
CALL sp_check_table_scd_mismatches('user_contact_dim','contact','scd_diff_contact_table');


-- Getting the combined results
SELECT 'address' as key_type, address as key_value, scd_mismatches FROM scd_diff_address_table
UNION
SELECT 'contact' as key_type, contact as key_value, scd_mismatches FROM scd_diff_contact_table;


-- Dropping the created temp tables and stored procedure
DROP TABLE IF EXISTS user_address_dim;
DROP TABLE IF EXISTS user_contact_dim;
DROP TABLE IF EXISTS scd_diff_address_table;
DROP TABLE IF EXISTS scd_diff_contact_table;
DROP PROCEDURE sp_check_table_scd_mismatches;
