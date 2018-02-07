package org.ohdsi.webapi.cohort;

import org.ohdsi.webapi.cohortdefinition.CohortGenerationResults.Cohort;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CohortMapper implements RowMapper<Cohort> {
    @Override
    public Cohort mapRow(ResultSet rs, int i) throws SQLException {
        Cohort resultItem = new Cohort();
        resultItem.id = rs.getLong("subject_id");
        resultItem.startDate = rs.getDate("cohort_start_date");
        resultItem.endDate = rs.getDate("cohort_end_date");

        return resultItem;
    }
}
