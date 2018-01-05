SELECT DISTINCT CONCEPT_ID, CONCEPT_NAME
INTO @storeData
FROM (
  {@expandConcepts==1}?{
    SELECT DISTINCT c1.CONCEPT_ID, c1.CONCEPT_NAME
    FROM @vocabulary.CONCEPT_ANCESTOR ca1
    	JOIN @vocabulary.CONCEPT c1
    		ON c1.CONCEPT_ID = ca1.DESCENDANT_CONCEPT_ID
    WHERE ANCESTOR_CONCEPT_ID IN (	{@concepts!=''}?{@concepts}:{0} )
    UNION ALL
    SELECT DISTINCT c1.CONCEPT_ID, c1.CONCEPT_NAME
    FROM @vocabulary.CONCEPT_RELATIONSHIP ca1
    	JOIN @vocabulary.CONCEPT c1
    		ON c1.CONCEPT_ID = ca1.CONCEPT_ID_1
    WHERE ca1.CONCEPT_ID_2 IN (	{@concepts!=''}?{@concepts}:{0} )
  }:{
    SELECT DISTINCT c1.CONCEPT_ID, c1.CONCEPT_NAME
    FROM @vocabulary.CONCEPT c1
    WHERE CONCEPT_ID IN (	{@concepts!=''}?{@concepts}:{0} )
  }
) z;
