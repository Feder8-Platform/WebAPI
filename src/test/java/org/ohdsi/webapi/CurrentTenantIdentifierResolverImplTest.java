package org.ohdsi.webapi;

import org.junit.Before;
import org.junit.Test;
import org.ohdsi.webapi.source.SourceDaimon;
import org.ohdsi.webapi.source.SourceDaimonContext;

import static org.junit.Assert.*;

/**
 * @author Peter Moorthamer
 * Date: 02/feb/2018
 */
public class CurrentTenantIdentifierResolverImplTest {

    @Before
    public void before() {
        SourceDaimonContextHolder.clear();
    }

    @Test
    public void resolveCurrentTenantIdentifierDefault() {
        CurrentTenantIdentifierResolverImpl resolver = new CurrentTenantIdentifierResolverImpl();
        assertEquals(DataSourceLookup.PRIMARY_DATA_SOURCE_KEY, resolver.resolveCurrentTenantIdentifier());
    }

    @Test
    public void resolveCurrentTenantIdentifierContext() {
        CurrentTenantIdentifierResolverImpl resolver = new CurrentTenantIdentifierResolverImpl();
        SourceDaimonContext context = new SourceDaimonContext("TEST", SourceDaimon.DaimonType.Results);
        SourceDaimonContextHolder.setCurrentSourceDaimonContext(context);
        assertEquals(context.getSourceDaimonContextKey(), resolver.resolveCurrentTenantIdentifier());
    }

    @Test
    public void validateExistingCurrentSessions() {
        CurrentTenantIdentifierResolverImpl resolver = new CurrentTenantIdentifierResolverImpl();
        assertTrue(resolver.validateExistingCurrentSessions());
    }
}