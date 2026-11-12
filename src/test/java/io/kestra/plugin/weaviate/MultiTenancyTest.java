package io.kestra.plugin.weaviate;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.weaviate.SchemaCreate.MultiTenancyOptions;

import io.weaviate.client6.v1.api.collections.tenants.Tenant;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MultiTenancyTest extends WeaviateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    public void tenantsAreIsolated() throws Exception {
        RunContext runContext = runContextFactory.of();

        var created = schemaCreate()
            .fields(Property.ofValue(Map.of("name", "text")))
            .multiTenancy(Property.ofValue(MultiTenancyOptions.builder().tenants(List.of("acme", "globex")).build()))
            .skipIfExists(Property.ofValue(true))
            .build()
            .run(runContext);
        assertThat(created.getCreated(), is(true));
        assertThat(created.getTenantsCreated(), is(List.of("acme", "globex")));

        try (var client = client()) {
            var config = client.collections.getConfig(CLASS_NAME).orElseThrow().multiTenancy();
            assertThat(config.enabled(), is(true));
            assertThat(tenants(), is(Set.of("acme", "globex")));
        }

        batchCreate().tenant(Property.ofValue("acme")).objects(List.of(Map.of("name", "a1"), Map.of("name", "a2"))).build().run(runContext);
        batchCreate().tenant(Property.ofValue("globex")).objects(List.of(Map.of("name", "g1"))).build().run(runContext);

        assertThat(names(runContext, "acme"), is(Set.of("a1", "a2")));
        assertThat(names(runContext, "globex"), is(Set.of("g1")));

        var deleted = delete().tenant(Property.ofValue("acme")).filter(Property.ofValue(Map.of("name", "a1"))).build().run(runContext);
        assertThat(deleted.getDeletedCount(), is(1L));
        assertThat(names(runContext, "acme"), is(Set.of("a2")));
        assertThat(names(runContext, "globex"), is(Set.of("g1")));

        var missingTenant = assertThrows(Exception.class, () -> query().build().run(runContext));
        assertThat(missingTenant.getMessage(), containsString("tenant"));

        // Without autoTenantCreation, an unknown tenant is an error.
        assertThrows(
            Exception.class,
            () -> batchCreate().tenant(Property.ofValue("initech")).objects(List.of(Map.of("name", "i1"))).build().run(runContext)
        );

        var rerun = schemaCreate()
            .fields(Property.ofValue(Map.of("name", "text")))
            .multiTenancy(Property.ofValue(MultiTenancyOptions.builder().tenants(List.of("acme", "initech")).build()))
            .skipIfExists(Property.ofValue(true))
            .build()
            .run(runContext);
        assertThat(rerun.getCreated(), is(false));
        assertThat(rerun.getTenantsCreated(), is(List.of("initech")));
        assertThat(tenants(), is(Set.of("acme", "globex", "initech")));
    }

    @Test
    public void autoTenantCreationCreatesTenantsOnInsert() throws Exception {
        RunContext runContext = runContextFactory.of();

        schemaCreate()
            .fields(Property.ofValue(Map.of("name", "text")))
            .multiTenancy(Property.ofValue(MultiTenancyOptions.builder().autoTenantCreation(true).autoTenantActivation(true).build()))
            .build()
            .run(runContext);

        try (var client = client()) {
            var config = client.collections.getConfig(CLASS_NAME).orElseThrow().multiTenancy();
            assertThat(config.createAutomatically(), is(true));
            assertThat(config.activateAutomatically(), is(true));
        }

        batchCreate().tenant(Property.ofValue("newco")).objects(List.of(Map.of("name", "n1"))).build().run(runContext);
        assertThat(tenants(), is(Set.of("newco")));
        assertThat(names(runContext, "newco"), is(Set.of("n1")));
    }

    @Test
    public void rejectsTenantsWithMultiTenancyDisabled() {
        var e = assertThrows(
            IllegalArgumentException.class,
            () -> SchemaCreate.tenantNames(MultiTenancyOptions.builder().enabled(false).tenants(List.of("a")).build())
        );
        assertThat(e.getMessage(), containsString("needs multi-tenancy"));
        assertThat(SchemaCreate.tenantNames(MultiTenancyOptions.builder().tenants(List.of("a", "b", "a")).build()), is(List.of("a", "b")));
    }

    private static Set<String> tenants() throws Exception {
        try (var client = client()) {
            return client.collections.use(CLASS_NAME).tenants.list().stream().map(Tenant::name).collect(Collectors.toSet());
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> names(RunContext runContext, String tenant) throws Exception {
        return query()
            .tenant(Property.ofValue(tenant))
            .limit(Property.ofValue(100))
            .build()
            .run(runContext)
            .getRows()
            .stream()
            .map(r -> (String) properties((Map<String, Object>) r).get("name"))
            .collect(Collectors.toSet());
    }
}
