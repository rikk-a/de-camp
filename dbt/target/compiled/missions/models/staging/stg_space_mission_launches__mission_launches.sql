select
    `organisation`                    as  `organisation`
,   `location`                        as  `location`
,   `date`                            as  `date`
,   `detail`                          as  `detail`
,   `rocket_status`                   as  `rocket_status`
,   `price`                           as  `price`
,   `mission_status`                  as  `mission_status`
,   `country`                         as  `country`
from `de-camp-v1`.`space_mission_launches`.`mission_launches`
order by `date`