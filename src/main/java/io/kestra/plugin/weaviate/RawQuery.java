package io.kestra.plugin.weaviate;

import com.google.gson.internal.LinkedTreeMap;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.graphql.model.GraphQLResponse;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotBlank;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(title = "GraphQL query request to Weaviate database.")
@Plugin(examples = {
    @Example(title = "Send a GraphQL raw query request to a Weaviate database, which allows you to retrieve data from the database", code = {

    })
})
public class RawQuery extends WeaviateConnection implements RunnableTask<RawQuery.Output>, RawQueryInterface {

    @NotBlank
    protected String query;

    @Builder.Default
    protected boolean store = false;

    @Override
    public RawQuery.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        Result<GraphQLResponse> result = client.graphQL().raw().withQuery(runContext.render(query)).run();

        Map<String, Object> data = extractData(result);

        int size = data.values().stream().map(object -> (List<Object>) object).findAny().map(List::size).orElse(0);

        return Output.builder().data(data).size(size).build();
    }

    private Map<String, Object> extractData(Result<GraphQLResponse> result) {
        String responseKey = "Get";

        Object data = result.getResult().getData();
        LinkedTreeMap<String, Object> dataMap = (LinkedTreeMap<String, Object>) data;
        return (Map<String, Object>) dataMap.getOrDefault(responseKey, new LinkedHashMap<>());
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Map containing the fetched data"
        )
        private Map<String, Object> data;

        @Schema(
            title = "The URI of the stored result"
        )
        private URI uri;

        @Schema(
            title = "The amount of rows fetched"
        )
        private int size;

    }
}
