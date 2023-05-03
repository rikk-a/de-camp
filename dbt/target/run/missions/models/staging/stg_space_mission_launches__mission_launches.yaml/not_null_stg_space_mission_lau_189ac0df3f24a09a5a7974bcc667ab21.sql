select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select organisation
from `de-camp-v1`.`mission_launches_dataset`.`stg_space_mission_launches__mission_launches`
where organisation is null



      
    ) dbt_internal_test