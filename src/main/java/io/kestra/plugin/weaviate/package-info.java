@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for using Weaviate database.\n"+
        "Weaviate is an vector database. It allows you to store data objects and vector embeddings from your favorite ML-models",categories = {
        PluginSubGroup.PluginCategory.DATA,
        PluginSubGroup.PluginCategory.AI
    }
)
package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.PluginSubGroup;