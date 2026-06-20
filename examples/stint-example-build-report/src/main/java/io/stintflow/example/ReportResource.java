package io.stintflow.example;

import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.WorkflowEngine;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/** Trigger ingress: POST a report request, get the final workflow output when it completes. */
@Path("/reports")
public class ReportResource {

    @Inject
    WorkflowEngine engine;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public CompletionStage<JsonNode> build(JsonNode input) {
        return engine.startAndWait(BuildReport.REF, input);
    }
}
