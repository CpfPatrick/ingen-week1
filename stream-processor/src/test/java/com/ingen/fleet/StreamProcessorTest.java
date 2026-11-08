package com.ingen.fleet;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamProcessorTest {
    @Test void exposesDraftStatus() {
        assertEquals("NOT_IMPLEMENTED_WEEK_1", StreamProcessor.status());
    }
}
