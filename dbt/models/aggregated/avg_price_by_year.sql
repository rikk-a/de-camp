SELECT
    date_trunc(date, YEAR)     as `year`
,   round(avg(price),6)        as avg_price
FROM
    {{ ref('stg_space_mission_launches__mission_launches') }}
group by date
order by date
