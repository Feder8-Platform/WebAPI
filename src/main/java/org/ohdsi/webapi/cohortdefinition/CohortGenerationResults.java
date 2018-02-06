package org.ohdsi.webapi.cohortdefinition;

import java.util.Date;
import java.util.List;

public class CohortGenerationResults {
    public static class Cohort {
        public long id;
        public Date startDate;
        public Date endDate;
    }
    public static class CohortInclusion {
        public int ruleSequence;
        public String name;
        public String description;
    }
    public static class CohortInclusionResult {
        public long inclusionRuleMask;
        public long personCount;
    }
    public static class CohortInclusionStats {
        public int ruleSequence;
        public long personCount;
        public long gainCount;
        public long personTotal;
    }
    public static class CohortSummaryStats {
        public long baseCount;
        public long finalCount;
    }

    public List<Cohort> cohort;
    public List<CohortInclusion> cohortInclusion;
    public List<CohortInclusionResult> cohortInclusionResult;
    public List<CohortInclusionStats> cohortInclusionStats;
    public List<CohortSummaryStats> cohortSummaryStats;
    public List<CohortGenerationInfo> cohortGenerationInfo;
}
