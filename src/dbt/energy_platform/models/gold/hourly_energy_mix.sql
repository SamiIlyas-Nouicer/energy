with base as (
    select * from {{ ref('stg_energy') }}
),

hourly as (
    select
        date_trunc('hour', timestamp)   as hour,
        date,
        -- Convert MW readings to MWh (30-min interval → divide by 2)
        sum(nuclear_mw) / 2             as nuclear_mwh,
        sum(solar_mw) / 2               as solar_mwh,
        sum(wind_mw) / 2                as wind_mwh,
        sum(hydro_mw) / 2               as hydro_mwh,
        sum(bio_mw) / 2                 as bio_mwh,
        sum(gas_mw) / 2                 as gas_mwh,
        sum(coal_mw) / 2                as coal_mwh,
        sum(oil_mw) / 2                 as oil_mwh,
        sum(total_production_mw) / 2    as total_production_mwh,
        sum(renewable_production_mw) / 2 as renewable_production_mwh,
        sum(consumption_mw) / 2         as consumption_mwh,
        avg(renewable_share_pct)        as renewable_share_pct,

        -- 7-day rolling average of renewable share (168 hourly slots)
        avg(avg(renewable_share_pct)) over (
            order by date_trunc('hour', timestamp)
            rows between 167 preceding and current row
        ) as renewable_share_7d_avg,

        round(sum(nuclear_mw) / nullif(sum(total_production_mw), 0) * 100, 2)
            as nuclear_share_pct,
        round(sum(gas_mw + coal_mw + oil_mw) / nullif(sum(total_production_mw), 0) * 100, 2)
            as fossil_share_pct

    from base
    group by 1, 2
)

select * from hourly
order by hour