package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.Map;

public interface BatchCreateInterface {

    @Schema(
        title = "Class name where you want to insert data"
    )
    @PluginProperty
    String getClassName();

    @Schema(
        title = "Parameters which will be inserted into the class"
    )
    @PluginProperty
    List<Map<String, Object>> getParameters();
}
