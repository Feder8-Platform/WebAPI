select 'Length of observation' as series_name, 
	cast(hr1.stratum_1 as int)*30 as x_length_of_observation, 
	round(1.0*sum(ar2.count_value) / denom.count_value,5) as y_percent_persons
from (select * from @resultsSchema.heracles_results where analysis_id = 108 and cohort_definition_id in (@cohortDefinitionId)) hr1
inner join
(
	select * from @resultsSchema.heracles_results where analysis_id = 108 and cohort_definition_id in (@cohortDefinitionId)
) ar2 on hr1.analysis_id = ar2.analysis_id and cast(hr1.stratum_1 as int) <= cast(ar2.stratum_1 as int),
(
	select count_value from @resultsSchema.heracles_results where analysis_id = 1 and cohort_definition_id in (@cohortDefinitionId)
) denom
group by cast(hr1.stratum_1 as int)*30, denom.count_value
order by cast(hr1.stratum_1 as int)*30 asc
