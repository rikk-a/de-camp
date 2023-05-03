

  create or replace view `de-camp-v1`.`mission_launches_dataset`.`agg_count_price_by_organisations`
  OPTIONS()
  as SELECT
    organisation
,   mission_status
,   count(*) as count_by_status
,   round(sum(price),6) as price
FROM
    `de-camp-v1`.`mission_launches_dataset`.`stg_space_mission_launches__mission_launches`
group by organisation , mission_status
order by organisation,mission_status;

