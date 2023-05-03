

  create or replace view `de-camp-v1`.`mission_launches_dataset`.`avg_price_by_year`
  OPTIONS()
  as SELECT
    date_trunc(date, YEAR)     as `year`
,   round(avg(price),6)        as avg_price
FROM
    `de-camp-v1`.`mission_launches_dataset`.`stg_space_mission_launches__mission_launches`
group by date
order by date;

