package org.ohdsi.webapi;

import org.junit.Before;
import org.junit.Test;
import org.ohdsi.webapi.source.Source;
import org.ohdsi.webapi.source.SourceDaimon;
import org.ohdsi.webapi.source.SourceDaimonContext;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.lookup.DataSourceLookupFailureException;

import javax.sql.DataSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Peter Moorthamer
 * Date: 02/feb/2018
 */
public class DataSourceLookupTest {

    private static final String SOURCE_KEY = "TEST";
    private static final String SOURCE_DIALECT = "postgresql";

    private DataSourceLookup dataSourceLookup;
    private DataSource primaryDataSource;

    @Before
    public void setup() {
        dataSourceLookup = new DataSourceLookup();
        primaryDataSource = new DriverManagerDataSource("primaryDataSourceConnectString");
        dataSourceLookup.setPrimaryDataSource(primaryDataSource);
    }

    @Test
    public void initDataSources() {
        dataSourceLookup.initDataSources(generateDummySources());

        assertEquals(2, dataSourceLookup.getDataSourceCount());

        DataSource resultsDataSource = dataSourceLookup.getDataSource(new SourceDaimonContext(SOURCE_KEY, SourceDaimon.DaimonType.Results).getSourceDaimonContextKey());
        assertNotNull(resultsDataSource);
        assertTrue(resultsDataSource instanceof DriverManagerDataSource);

        DataSource vocabularyDataSource = dataSourceLookup.getDataSource(new SourceDaimonContext(SOURCE_KEY, SourceDaimon.DaimonType.Vocabulary).getSourceDaimonContextKey());
        assertNotNull(vocabularyDataSource);
        assertTrue(vocabularyDataSource instanceof DriverManagerDataSource);

        assertNotEquals(primaryDataSource, resultsDataSource);
        assertNotEquals(primaryDataSource, vocabularyDataSource);
        assertNotEquals(resultsDataSource, vocabularyDataSource);
    }

    @Test
    public void getDataSourceCount() {
        assertEquals(0, dataSourceLookup.getDataSourceCount());
    }

    @Test(expected = DataSourceLookupFailureException.class)
    public void getDataSourceInvalidKey() {
        assertEquals(primaryDataSource, dataSourceLookup.getDataSource(SOURCE_KEY));
    }

    @Test()
    public void getDataSourceBeforeInit() {
        SourceDaimonContext ctx = new SourceDaimonContext(SOURCE_KEY, SourceDaimon.DaimonType.Results);
        assertEquals(primaryDataSource, dataSourceLookup.getDataSource(ctx.getSourceDaimonContextKey()));
    }

    @Test
    public void getPrimaryDataSource() {
        assertEquals(primaryDataSource, dataSourceLookup.getPrimaryDataSource());
    }

    private List<Source> generateDummySources() {
        List<Source> sources = new ArrayList<>();

        Source source = new Source();
        source.setSourceKey(SOURCE_KEY);
        source.setSourceDialect(SOURCE_DIALECT);
        source.setSourceConnection("dummyConnectionString");

        List<SourceDaimon> sourceDaimons = new ArrayList<>();

        SourceDaimon resultsDaimon = new SourceDaimon();
        resultsDaimon.setDaimonType(SourceDaimon.DaimonType.Results);
        resultsDaimon.setTableQualifier(SourceDaimon.DaimonType.Results.name());
        sourceDaimons.add(resultsDaimon);

        SourceDaimon vocDaimon = new SourceDaimon();
        vocDaimon.setDaimonType(SourceDaimon.DaimonType.Vocabulary);
        vocDaimon.setTableQualifier(SourceDaimon.DaimonType.Vocabulary.name());
        sourceDaimons.add(vocDaimon);

        source.setDaimons(sourceDaimons);
        sources.add(source);

        return sources;
    }
}