SELECT
    organisation
,   mission_status
,   count(*) as count_by_status
,   round(sum(price),6) as price
FROM
    {{ ref('stg_space_mission_launches__mission_launches') }}
group by organisation , mission_status
order by organisation, mission_status
