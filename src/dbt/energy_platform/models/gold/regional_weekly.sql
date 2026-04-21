{{ config(
    materialized='external',
    location="s3://gold/{{ this.name }}/",
    format='parquet'
) }}

with base as (
    select * from {{ ref('stg_energy') }}
),

-- Since the RTE consumption API does not provide regional breakdown,
-- we derive regional proxies from the national total using
-- official INSEE 2023 population weights per metropolitan region.
-- This approximates per-capita consumption until the regional endpoint
-- is connected.
population_weights as (
    select 'Île-de-France'            as region, 12_271_794 as population union all
    select 'Auvergne-Rhône-Alpes',                8_037_842              union all
    select 'Nouvelle-Aquitaine',                  6_109_875              union all
    select 'Occitanie',                           6_066_546              union all
    select 'Hauts-de-France',                     6_002_634              union all
    select 'Grand Est',                           5_579_813              union all
    select 'Provence-Alpes-Côte d''Azur',         5_175_543              union all
    select 'Pays de la Loire',                    3_870_240              union all
    select 'Normandie',                           3_303_500              union all
    select 'Bretagne',                            3_394_221              union all
    select 'Bourgogne-Franche-Comté',             2_797_014              union all
    select 'Centre-Val de Loire',                 2_572_853              union all
    select 'Corse',                                 351_255
),

weekly_national as (
    select
        date_trunc('week', date)                    as week_start,
        round(sum(consumption_mw) / 2000.0, 2)     as national_consumption_gwh,
        round(avg(renewable_share_pct), 2)          as avg_renewable_share_pct

    from base
    group by 1
),

regional as (
    select
        w.week_start,
        p.region,
        p.population,
        -- Distribute national consumption by population weight
        round(
            w.national_consumption_gwh *
            (p.population::double / 67_531_130.0),
        4) as regional_consumption_gwh,

        round(
            w.national_consumption_gwh *
            (p.population::double / 67_531_130.0) *
            1_000_000.0 / p.population,
        4) as consumption_kwh_per_capita,

        w.avg_renewable_share_pct

    from weekly_national w
    cross join population_weights p
)

select * from regional
order by week_start, consumption_kwh_per_capita desc