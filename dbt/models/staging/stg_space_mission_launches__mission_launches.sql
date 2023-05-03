select
    `organisation`                    as  `organisation`
,   `location`                        as  `location`
,   `date`                            as  `date`
,   `detail`                          as  `detail`
,   `rocket_status`                   as  `rocket_status`
,   `price`                           as  `price`
,   `mission_status`                  as  `mission_status`
,   `country`                         as  `country`
from {{ source('staging', 'mission_launches') }}
order by `date`