package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotBlank;

public interface RawQueryInterface {

    @Schema(
        title = "GraphQL query which will be executed"
    )
    @NotBlank
    @PluginProperty
    String getQuery();

    @Schema(
        title = "Whether store data in internal storage. Default is falses"
    )
    @PluginProperty
    boolean isStore();
}
