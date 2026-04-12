with source as (
    select *
    from read_parquet('/home/sami/Desktop/energy/src/data/silver_export/*.parquet')
),

renamed as (
    select
        timezone('Europe/Paris', timestamp::timestamptz) as timestamp,
        date_trunc('day', timezone('Europe/Paris', timestamp::timestamptz)) as date,
        date_part('hour', timezone('Europe/Paris', timestamp::timestamptz)) as hour_of_day,
        date_part('dow', timezone('Europe/Paris', timestamp::timestamptz)) as day_of_week,

        coalesce(nuclear_mw, 0)              as nuclear_mw,
        coalesce(wind_mw, 0)                 as wind_mw,
        coalesce(solar_mw, 0)                as solar_mw,
        coalesce(hydro_mw, 0)                as hydro_mw,
        coalesce(bio_mw, 0)                  as bio_mw,
        coalesce(gas_mw, 0)                  as gas_mw,
        coalesce(coal_mw, 0)                 as coal_mw,
        coalesce(oil_mw, 0)                  as oil_mw,
        consumption_mw,
        coalesce(total_production_mw, 0)     as total_production_mw,
        coalesce(renewable_production_mw, 0) as renewable_production_mw,
        coalesce(renewable_share_pct, 0)     as renewable_share_pct,
        temperature_celsius,
        heating_degree_days,
        is_complete

    from source
    where consumption_mw is not null
      and timestamp is not null
),

deduplicated as (
    select *,
        row_number() over (partition by timestamp order by timestamp) as rn
    from renamed
)

select
    timestamp,
    date,
    hour_of_day,
    day_of_week,
    nuclear_mw,
    wind_mw,
    solar_mw,
    hydro_mw,
    bio_mw,
    gas_mw,
    coal_mw,
    oil_mw,
    consumption_mw,
    total_production_mw,
    renewable_production_mw,
    renewable_share_pct,
    temperature_celsius,
    heating_degree_days,
    is_complete
from deduplicated
where rn = 1