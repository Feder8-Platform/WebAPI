package org.ohdsi.webapi.source;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Peter Moorthamer
 * Date: 02/feb/2018
 */
public class SourceDaimonContextTest {

    @Test
    public void testSourceDaimonContextKey() {
        SourceDaimonContext sdc = new SourceDaimonContext("test_key:Results");
        assertEquals("test_key", sdc.getSourceKey());
        assertEquals(SourceDaimon.DaimonType.Results, sdc.getDaimonType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSourceDaimonContextKeyInvalid() {
        new SourceDaimonContext("invalidKey");
    }
    @Test(expected = IllegalArgumentException.class)
    public void testSourceDaimonContextKeyInvalidNull() {
        new SourceDaimonContext(null);
    }
    @Test(expected = IllegalArgumentException.class)
    public void testSourceDaimonContextKeyInvalidBlank() {
        new SourceDaimonContext(" ");
    }
    @Test(expected = IllegalArgumentException.class)
    public void testSourceDaimonContextKeyInvalidLength() {
        new SourceDaimonContext("test_key:Results:CDM");
    }
    @Test(expected = IllegalArgumentException.class)
    public void testSourceDaimonContextKeyInvalidDaimonType() {
        new SourceDaimonContext("test_key:test");
    }

    @Test
    public void getSourceDaimonContextKey() {
        SourceDaimonContext sdc = new SourceDaimonContext("test_key", SourceDaimon.DaimonType.Results);
        assertEquals("test_key:Results", sdc.getSourceDaimonContextKey());
    }

    @Test
    public void testToString() {
        SourceDaimonContext sdc = new SourceDaimonContext("test_key", SourceDaimon.DaimonType.Results);
        assertEquals("test_key:Results", sdc.toString());
    }
}