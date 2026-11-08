package com.ingen.fleet;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DependenciesHealthTest {
    private Admin healthyAdmin() {
        Admin admin = mock(Admin.class);
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(admin.describeCluster()).thenReturn(cluster);
        when(cluster.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(new Node(1, "host", 9092))));
        return admin;
    }
    @Test void healthyDependenciesAndBoundedClose() {
        Admin admin = healthyAdmin();
        assertEquals(Status.UP, new DependenciesHealth(() -> admin, () -> true).health().getStatus());
        verify(admin).close(Duration.ofMillis(500));
    }
    @Test void kafkaFailureThenRecovery() {
        AtomicBoolean available = new AtomicBoolean(false);
        Admin admin = healthyAdmin();
        var probe = new DependenciesHealth(() -> {
            if (!available.get()) { throw new IllegalStateException("offline"); }
            return admin;
        }, () -> true);
        assertEquals(Status.DOWN, probe.health().getStatus());
        available.set(true);
        assertEquals(Status.UP, probe.health().getStatus());
    }
    @Test void registryFailureThenRecovery() {
        AtomicBoolean available = new AtomicBoolean(false);
        Admin admin = healthyAdmin();
        var probe = new DependenciesHealth(() -> admin, available::get);
        assertEquals(Status.DOWN, probe.health().getStatus());
        available.set(true);
        assertEquals(Status.UP, probe.health().getStatus());
    }
    @Test void queryFailureStillClosesAdmin() {
        Admin admin = mock(Admin.class);
        when(admin.describeCluster()).thenThrow(new IllegalStateException("offline"));
        assertEquals(Status.DOWN, new DependenciesHealth(() -> admin, () -> true).health().getStatus());
        verify(admin).close(Duration.ofMillis(500));
    }
}
