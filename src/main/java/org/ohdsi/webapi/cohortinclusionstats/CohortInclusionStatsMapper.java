package org.ohdsi.webapi.cohortinclusionstats;

import org.ohdsi.webapi.cohortdefinition.CohortGenerationResults.CohortInclusionStats;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CohortInclusionStatsMapper implements RowMapper<CohortInclusionStats> {
    @Override
    public CohortInclusionStats mapRow(ResultSet rs, int i) throws SQLException {
        CohortInclusionStats resultItem = new CohortInclusionStats();
        resultItem.gainCount = rs.getLong("gain_count");
        resultItem.personCount = rs.getLong("person_count");
        resultItem.personTotal = rs.getLong("person_total");
        resultItem.ruleSequence = rs.getInt("rule_sequence");
        return resultItem;
    }
}
