# 04-Analytics-Engineering
Data Engineering Zoomcamp week 4 - Analytics Engineering

## Homework 4

##Question 1: int_trrip_unioned only

##Question 2:dbt fails the test with non-zero exit code

##Question 3 

    select count (*)
    from prod.fct_monthly_zone_revenue
    ;
    
##Question 4

        select 
            pickup_zone,
            sum(revenue_monthly_total_amount) as total_revenue
        from prod.fct_monthly_zone_revenue
        where 
            service_type = 'Green'
        and extract(year from revenue_month) = 2020
        group by pickup_zone
        order by total_revenue desc
;

##Question 5:

        select 
            sum(total_monthly_trips) as total_green_taxi_trips
        from prod.fct_monthly_zone_revenue
        where 
            service_type = 'Green'
            and revenue_month = '2019-10-01'
    ;
    
##Question 6 : Please refer to python script; ingest_fhv.py

        with base as (
            select dispatching_base_num,
                    pickup_datetime,
                    dropOff_datetime as dropoff_datetime,
                    PUlocationID as pickup_location_id,
                    DOlocationID as dropoff_location_id,
                    SR_Flag,
                    Affiliated_base_number
            from {{ source('raw_data', 'fhv_tripdata')}}
        )
        select *
        from base 
        where dispatching_base_num IS not NULL
        ;
        select (*)
        from prod.stg_fhv_tripdata
