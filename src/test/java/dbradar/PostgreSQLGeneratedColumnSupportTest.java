package dbradar;

import dbradar.postgresql.PostgreSQLGeneratedColumnSupport;
import dbradar.postgresql.PostgreSQLGeneratedColumnSupport.GeneratedColumnKind;

import java.util.List;

public final class PostgreSQLGeneratedColumnSupportTest {

    private PostgreSQLGeneratedColumnSupportTest() {
    }

    public static void main(String[] args) {
        verifyVirtualVersionGate();
        verifyClauseRendering();
    }

    private static void verifyVirtualVersionGate() {
        require(!PostgreSQLGeneratedColumnSupport.supportsVirtualGeneratedColumns(160013),
                "Expected PostgreSQL 16 to reject virtual generated-column support");
        require(PostgreSQLGeneratedColumnSupport.supportsVirtualGeneratedColumns(180000),
                "Expected PostgreSQL 18 to enable virtual generated-column support");

        require(PostgreSQLGeneratedColumnSupport.getBootstrapGeneratedColumnKinds(160013, 5)
                        .equals(List.of(GeneratedColumnKind.STORED)),
                "Expected PostgreSQL 16 bootstrap to keep only the stored generated column");
        require(PostgreSQLGeneratedColumnSupport.getBootstrapGeneratedColumnKinds(180000, 4)
                        .equals(List.of(GeneratedColumnKind.STORED)),
                "Expected PostgreSQL 18 to preserve task5 type coverage when ddl-count is only 4");
        require(PostgreSQLGeneratedColumnSupport.getBootstrapGeneratedColumnKinds(180000, 5)
                        .equals(List.of(GeneratedColumnKind.STORED, GeneratedColumnKind.VIRTUAL)),
                "Expected PostgreSQL 18 bootstrap to add a virtual generated column once ddl-count reaches 5");
    }

    private static void verifyClauseRendering() {
        require("GENERATED ALWAYS AS (c1 + c2) STORED".equals(
                        PostgreSQLGeneratedColumnSupport.renderGeneratedColumnClause("c1 + c2",
                                GeneratedColumnKind.STORED)),
                "Expected the stored generated-column clause to render correctly");
        require("GENERATED ALWAYS AS (c1 + c2) VIRTUAL".equals(
                        PostgreSQLGeneratedColumnSupport.renderGeneratedColumnClause("c1 + c2",
                                GeneratedColumnKind.VIRTUAL)),
                "Expected the virtual generated-column clause to render correctly");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
