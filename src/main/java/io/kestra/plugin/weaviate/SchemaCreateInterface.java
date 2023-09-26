package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.Map;

public interface SchemaCreateInterface {

    @Schema(
        title = "Class name where you will be data stored"
    )
    @PluginProperty
    String getClassName();

    @Schema(
        title = "Fields which will be store data in class",
        description = "Requires specified field name and list of data type that will be stored in this field"
    )
    @PluginProperty
    Map<String, List<String>> getParameters();
}
