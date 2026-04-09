package dbradar.mysql;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMySQLTablespaceExecutionGate {

    @Test
    public void testSensitiveSqlConcurrencyIsCappedAtThirty() throws Exception {
        MySQLTablespaceGate.resetForTests();

        int workers = 40;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch attemptsFinished = new CountDownLatch(workers);
        CountDownLatch done = new CountDownLatch(workers);
        AtomicInteger current = new AtomicInteger();
        AtomicInteger max = new AtomicInteger();
        AtomicInteger acquired = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < workers; i++) {
            Thread thread = new Thread(() -> {
                try {
                    start.await(1, TimeUnit.SECONDS);
                    MySQLTablespaceGate.GateLease lease = MySQLTablespaceGate.tryAcquire("CREATE TABLESPACE ts_demo");
                    if (lease.isAcquired()) {
                        acquired.incrementAndGet();
                        int inFlight = current.incrementAndGet();
                        max.accumulateAndGet(inFlight, Math::max);
                        attemptsFinished.countDown();
                        attemptsFinished.await(1, TimeUnit.SECONDS);
                        current.decrementAndGet();
                        lease.close();
                    } else {
                        attemptsFinished.countDown();
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            threads.add(thread);
            thread.start();
        }

        start.countDown();
        assertTrue(done.await(3, TimeUnit.SECONDS), "all gate test workers should finish");

        assertEquals(30, MySQLTablespaceGate.getMaxConcurrency(),
                "tablespace gate should expose the fixed max concurrency");
        assertEquals(30, acquired.get(),
                "with a single simultaneous wave, at most 30 workers should acquire the tablespace gate");
        assertTrue(max.get() <= 30,
                "tablespace gate should cap in-flight sensitive SQL at 30, but saw " + max.get());
    }

    @Test
    public void testNonTablespaceSqlBypassesGate() {
        MySQLTablespaceGate.resetForTests();
        List<MySQLTablespaceGate.GateLease> leases = new ArrayList<>();
        try {
            for (int i = 0; i < MySQLTablespaceGate.getMaxConcurrency(); i++) {
                MySQLTablespaceGate.GateLease lease = MySQLTablespaceGate.tryAcquire("DROP TABLESPACE ts_demo");
                assertTrue(lease.isAcquired(), "test setup should saturate the tablespace gate");
                leases.add(lease);
            }

            MySQLTablespaceGate.GateLease nonSensitiveLease =
                    MySQLTablespaceGate.tryAcquire("ALTER TABLE t0 ADD COLUMN c1 INT");
            assertTrue(nonSensitiveLease.isAcquired(),
                    "non-tablespace SQL should bypass the tablespace gate even when it is saturated");
            nonSensitiveLease.close();
        } finally {
            for (MySQLTablespaceGate.GateLease lease : leases) {
                lease.close();
            }
            MySQLTablespaceGate.resetForTests();
        }
    }
}
