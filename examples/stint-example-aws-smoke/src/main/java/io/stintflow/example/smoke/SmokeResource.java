package io.stintflow.example.smoke;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/** SDD 2.7: {@code POST /smoke {"name": "..."}} runs {@code aws-smoke} to completion and returns its context. */
@Path("/smoke")
public class SmokeResource {

    @Inject
    SmokeEngine smoke;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode run(JsonNode input) throws Exception {
        String id = smoke.engine().start(SmokeEngine.REF, input);
        return smoke.engine().awaitCompletion(id, Duration.ofSeconds(60)).toCompletableFuture().get(90, TimeUnit.SECONDS);
    }
}
