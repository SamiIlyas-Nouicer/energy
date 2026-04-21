{{ config(
    materialized='external',
    location="s3://gold/{{ this.name }}/",
    format='parquet'
) }}

with base as (
    select * from {{ ref('stg_energy') }}
),

-- Official RTE emission factors (gCO2/kWh)
emission_factors as (
    select 'nuclear'  as source, 6   as gco2_per_kwh union all
    select 'wind',              11                    union all
    select 'solar',             41                    union all
    select 'hydro',              6                    union all
    select 'bio',              230                    union all
    select 'gas',              490                    union all
    select 'coal',             820                    union all
    select 'oil',              730
),

co2 as (
    select
        timestamp,
        date,
        total_production_mw,
        renewable_share_pct,
        round(
            (nuclear_mw * 6 + wind_mw * 11 + solar_mw * 41 +
             hydro_mw * 6 + bio_mw * 230 + gas_mw * 490 +
             coal_mw * 820 + oil_mw * 730)
            / nullif(total_production_mw, 0), 2
        ) as co2_intensity_gco2_per_kwh,
        round(avg(
            (nuclear_mw * 6 + wind_mw * 11 + solar_mw * 41 +
             hydro_mw * 6 + bio_mw * 230 + gas_mw * 490 +
             coal_mw * 820 + oil_mw * 730)
            / nullif(total_production_mw, 0)
        ) over (
            order by timestamp
            rows between 47 preceding and current row
        ), 2) as co2_intensity_24h_avg
    from base
)

select * from co2
order by timestamp