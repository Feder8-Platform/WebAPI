package org.ohdsi.webapi.cohortinclusionresult;

import org.springframework.jdbc.core.RowMapper;
import org.ohdsi.webapi.cohortdefinition.CohortGenerationResults.CohortInclusionResult;

import java.sql.ResultSet;
import java.sql.SQLException;


public class CohortInclusionResultMapper implements RowMapper<CohortInclusionResult> {
    @Override
    public CohortInclusionResult mapRow(ResultSet rs, int i) throws SQLException {
        CohortInclusionResult resultItem = new CohortInclusionResult();
        resultItem.inclusionRuleMask = rs.getLong("inclusion_rule_mask");
        resultItem.personCount = rs.getLong("person_count");
        return resultItem;
    }
}
