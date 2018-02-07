package org.ohdsi.webapi.cohortinclusion;

import org.ohdsi.webapi.cohortdefinition.CohortGenerationResults.CohortInclusion;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CohortInclusionMapper implements RowMapper<CohortInclusion> {

    @Override
    public CohortInclusion mapRow(ResultSet rs, int i) throws SQLException {
        CohortInclusion resultItem = new CohortInclusion();
        resultItem.name = rs.getString("name");
        resultItem.description = rs.getString("description");
        resultItem.ruleSequence = rs.getInt("rule_sequence");
        return resultItem;
    }
}
