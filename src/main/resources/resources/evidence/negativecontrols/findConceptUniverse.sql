{DEFAULT @small_cell_count = 10}

{@outcomeOfInterest == 'condition'}?{
  SELECT c.CONCEPT_ID, c.CONCEPT_NAME,
  	SUM(u.PERSON_COUNT_ESTIMATE_RC) AS PERSON_COUNT_RC,
  	SUM(u.PERSON_COUNT_ESTIMATE_DC) AS PERSON_COUNT_DC
	INTO @storeData
  FROM @evidenceSchema.NC_LU_CONCEPT_UNIVERSE u
  	JOIN @vocabulary.CONCEPT c
  		ON c.CONCEPT_ID = u.CONDITION_CONCEPT_ID
  		AND LOWER(c.DOMAIN_ID) = LOWER('condition')
  WHERE DRUG_CONCEPT_ID IN (
  	@conceptsOfInterest
  )
  GROUP BY c.CONCEPT_ID, c.CONCEPT_NAME
  HAVING SUM(u.PERSON_COUNT_ESTIMATE_RC) > @small_cell_count
	;
}
{@outcomeOfInterest == 'drug'}?{
  SELECT c.CONCEPT_ID, c.CONCEPT_NAME,
  	SUM(u.PERSON_COUNT_ESTIMATE_RC) AS PERSON_COUNT_RC,
  	SUM(u.PERSON_COUNT_ESTIMATE_DC) AS PERSON_COUNT_DC
	INTO @storeData
  FROM @evidenceSchema.NC_LU_CONCEPT_UNIVERSE u
  	JOIN @vocabulary.CONCEPT c
  		ON c.CONCEPT_ID = u.DRUG_CONCEPT_ID
  		AND LOWER(c.DOMAIN_ID) = LOWER('drug')
  WHERE CONDITION_CONCEPT_ID IN (
  	@conceptsOfInterest
  )
  GROUP BY c.CONCEPT_ID, c.CONCEPT_NAME
  HAVING SUM(u.PERSON_COUNT_ESTIMATE_RC) > @small_cell_count
	;
}

CREATE INDEX tmp_cu_concept_id ON @storeData(concept_id);
CREATE INDEX tmp_cu_rc ON @storeData(person_count_rc);
