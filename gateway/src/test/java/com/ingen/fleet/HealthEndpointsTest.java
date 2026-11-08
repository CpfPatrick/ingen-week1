package com.ingen.fleet;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class HealthEndpointsTest {
    @Autowired private MockMvc mvc;
    @MockBean private DependenciesHealth dependencies;

    @Test void dependencyFailureChangesReadinessButNotLivenessThenRecovers() throws Exception {
        when(dependencies.getHealth(anyBoolean())).thenReturn(Health.down().build());
        mvc.perform(get("/actuator/health/readiness")).andExpect(status().isServiceUnavailable());
        mvc.perform(get("/actuator/health/liveness")).andExpect(status().isOk());
        when(dependencies.getHealth(anyBoolean())).thenReturn(Health.up().build());
        mvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk());
    }
}
