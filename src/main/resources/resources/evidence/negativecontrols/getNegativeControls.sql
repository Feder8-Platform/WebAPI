SELECT 
	job_id, 
	@SOURCE_ID source_id,
	@CONCEPT_SET_ID concept_set_id,
	'@CONCEPT_SET_NAME' concept_set_name,
	negative_control, 
	outcome_of_interest_concept_id concept_id, 
	outcome_of_interest_concept_name concept_name, 
	'@outcomeOfInterest' domain_id,
	person_count_rc, 
	person_count_dc, 
	descendant_pmid_count, 
	exact_pmid_count, 
	parent_pmid_count, 
	ancestor_pmid_count, 
	indication, 
	drug_induced, 
	splicer, 
	faers, 
	user_excluded, 
	user_included
FROM @evidenceSchema.nc_results
WHERE job_id = 	@jobId;