SELECT
    organisation
,   date_trunc(date, YEAR)     as `year`
,   round(avg(price))          as avg_price
FROM
    {{ ref('stg_space_mission_launches__mission_launches') }}
group by date, organisation
order by date, organisation
