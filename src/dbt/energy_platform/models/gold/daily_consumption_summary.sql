{{ config(
    materialized='external',
    location="s3://gold/{{ this.name }}/",
    format='parquet'
) }}

with base as (
    select * from {{ ref('stg_energy') }}
),

daily as (
    select
        date,
        round(sum(consumption_mw) / 2000.0, 4)  as total_consumption_gwh,
        round(max(consumption_mw) / 1000.0, 4)  as peak_consumption_gw,
        round(min(consumption_mw) / 1000.0, 4)  as off_peak_consumption_gw,
        round(avg(temperature_celsius), 2)       as avg_temperature_celsius,
        round(sum(heating_degree_days), 4)       as heating_degree_days_sum,
        round(avg(renewable_share_pct), 2)       as avg_renewable_share_pct
    from base
    group by 1
),

peak_hours as (
    select
        date,
        cast(date_part('hour', timestamp) as integer) as peak_consumption_hour,
        consumption_mw,
        row_number() over (
            partition by date
            order by consumption_mw desc
        ) as rn
    from base
)

select
    d.date,
    d.total_consumption_gwh,
    d.peak_consumption_gw,
    d.off_peak_consumption_gw,
    d.avg_temperature_celsius,
    d.heating_degree_days_sum,
    d.avg_renewable_share_pct,
    p.peak_consumption_hour
from daily d
left join peak_hours p
    on d.date = p.date
    and p.rn = 1
order by d.date