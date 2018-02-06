package org.ohdsi.webapi.cohortdefinition;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface CohortGenerationInfoRepository extends CrudRepository<CohortGenerationInfo, Long>{

    @Query("select cgi from CohortGenerationInfo cgi where cgi.id.cohortDefinitionId=?1")
    List<CohortGenerationInfo> listGenerationInfoById(Integer id);
}
