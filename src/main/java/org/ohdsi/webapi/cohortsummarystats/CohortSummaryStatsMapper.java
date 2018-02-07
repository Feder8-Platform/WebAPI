package org.ohdsi.webapi.cohortsummarystats;

import org.ohdsi.webapi.cohortdefinition.CohortGenerationResults.CohortSummaryStats;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CohortSummaryStatsMapper implements RowMapper<CohortSummaryStats> {
    @Override
    public CohortSummaryStats mapRow(ResultSet rs, int i) throws SQLException {
        CohortSummaryStats resultItem = new CohortSummaryStats();
        resultItem.baseCount= rs.getLong("base_count");
        resultItem.finalCount= rs.getLong("final_count");
        return resultItem;
    }
}
