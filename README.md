# Introduction
Slowly Changing Dimension Type 2 is the most common design feature for preserving history in dimensions within data warehouses. Therefore, it is integral to ensure that SCD2 is implemented correctly. This article provides a solution to identify errors in SCD2, which can be seamlessly integrated into your existing data pipelines or defined as a standalone data quality check in an automated manner.

### **What are dimensions in Data Warehouses?**

Dimensions in data warehousing are descriptive attributes of business entities, such as customers or products, providing context for numerical measures in fact tables. They typically consist of hierarchical structures and enable drill-down analysis, organizing data for meaningful insights and decision-making.

### **What are Slowly Changing Dimensions Type 2?**

It is a way of storing dimension tables that maintains multiple versions of each dimension record alongside two additional columns — effective date and expiry date. When a change occurs, a new record is inserted into the dimension table with an effective date equal to the current date and expiry date to be a far fetched date in the future such as ‘_9999–12–31_’. In addition, the expiry date of the previous version of the record is set to the current date. This way we can easily track the lineage of the dimension across time.

### **How can SCD Type 2 go wrong?**

SCD Type 2 can easily go wrong if the expiry date of a previous version record does not match with the effective date of its next version. This discrepancy can have a detrimental effect on downstream analytical tasks; therefore, such errors should be identified and fixed first to ensure accurate analysis.

Below are two examples showcasing SCD Type 2. In the first example the type 2 is done correctly while in the later it is incorrect and therefore must be fixed

<p align="center">
  <img src="/assets/scd_correct_example.png" />
</p>

<p align="center">
  <img src="/assets/scd_incorrect_example.png" />
</p>

### **How to identify incorrect SCD Type 2 records?**

The following stored procedure provides an automated way to check for SCD type 2 mismatches across multiple dimension tables in a jiffy. It is fully parameterized and only expects the dimension table name, the key dimensional attribute e.g address in a user address dimension table and finally the output table in which the results of the mismatches should be stored.

The output table would not only contain the dimension key value for which the mismatch happened but also the number of mismatches

**_Note:_** A mismatch is when the expiry date of a previous version does not match with the effective date of its next version

```sql
-- Creating Stored Procedure to automate faulty SCD 2 checks for all the tables  
CREATE OR REPLACE PROCEDURE sp_check_table_scd_mismatches(table_name varchar(512), key_name varchar(512), temp_table INOUT varchar(256))  
AS $$  
BEGIN  
   EXECUTE 'drop table if exists ' || temp_table;  
   EXECUTE 'create temp table ' || temp_table || ' as  
         with filtered_data as (  
            select   
                ' || key_name || '  
                ,bus_eff_dt  
                ,bus_exp_dt  
            from ' || table_name || '  
        ),   
        lag_dt_data as (  
            select *,  
                lag(bus_exp_dt,1) over(order by ' || key_name || ',bus_eff_dt,bus_exp_dt) as lag_dt   
            from filtered_data order by bus_eff_dt,bus_exp_dt  
        ),  
        scd_diff_data as (  
            select *  
                ,case when datediff(day,,lag_dt) <> 0 then 1 else 0 end as scd_diff  
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
```
Lets walk through an example to understand this better. Let us suppose that our dimension table is the following:

<p align="center">
  <img src="/assets/scd_input_data.png" />
</p>

We can clearly see that there are two rows for _Address = A_ where the SCD Type 2 is done incorrectly. Hence we use our SCD Type 2 check stored procedure on the above dimension we would get the following result

```sql
-- Calling the Stored Procedure with required parameters  
CALL sp_check_table_scd_mismatches('address_dimension','address','scd_diff_table');
  
-- Getting the results back  
SELECT * FROM scd_diff_table;
```

<p align="center">
  <img src="/assets/scd_mismatch_results.png" />
</p>

This output of the SCD Type 2 mismatch can be used as a feedback loop for our ETL step to ensure the required data quality using the following architecture

<p align="center">
  <img src="/assets/scd2_validation_architecture.png" />
</p>
