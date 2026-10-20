package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.stintflow.wire.Json;

/** SDD 3a.1, CA2: with the fake adapter, each option routes to its branch and an uncertain decision to onUncertain. */
class DecisionBranchingTest {

    static final String OUTCOMES = """
              - approved:
                  set: '${ {outcome: "approved"} }'
                  export: { as: '${ $context + . }' }
                  then: end
              - rejected:
                  set: '${ {outcome: "rejected"} }'
                  export: { as: '${ $context + . }' }
                  then: end
              - review:
                  set: '${ {outcome: "review"} }'
                  export: { as: '${ $context + . }' }
                  then: end
            """;

    static final String CLASSIFY = """
            do:
              - classify:
                  call: decision
                  with:
                    task: decide
                    kind: choice
                    input:
                      text: '${ .text }'
                    options:
                      - approve: { description: "The item was approved in the meeting", then: approved }
                      - reject: { description: "The item was rejected in the meeting", then: rejected }
                  onUncertain: review
            """ + OUTCOMES;

    @Test
    void each_option_routes_to_its_branch() throws Exception {
        DecisionFixture f = new DecisionFixture(CLASSIFY);

        assertThat(f.awaitCompleted(f.start("item 1: aprovado por todos")).get("outcome").asText()).isEqualTo("approved");
        assertThat(f.awaitCompleted(f.start("item 2: rejeitado")).get("outcome").asText()).isEqualTo("rejected");
        // The worker received the request: kind, options with their descriptions, and with.input.
        assertThat(f.requests.get(0)).isEqualTo(Json.read(("{\"kind\":\"choice\",\"options\":["
                + "{\"name\":\"approve\",\"description\":\"The item was approved in the meeting\"},"
                + "{\"name\":\"reject\",\"description\":\"The item was rejected in the meeting\"}],"
                + "\"input\":{\"text\":\"item 1: aprovado por todos\"}}").getBytes()));
    }

    @Test
    void uncertain_routes_to_on_uncertain() throws Exception {
        DecisionFixture f = new DecisionFixture(CLASSIFY);

        assertThat(f.awaitCompleted(f.start("item 3: incerto")).get("outcome").asText()).isEqualTo("review");
    }

    @Test
    void uncertain_without_on_uncertain_follows_the_value() throws Exception {
        DecisionFixture f = new DecisionFixture(CLASSIFY.replace("      onUncertain: review\n", ""));

        String id = f.start("item 3: incerto");
        assertThat(f.awaitCompleted(id).get("outcome").asText()).isEqualTo("approved");
        assertThat(f.decision(id, "/do/0/classify").get("uncertain").asBoolean()).isTrue();
    }

    @Test
    void boolean_and_score_route_as_specified() throws Exception {
        DecisionFixture booleans = new DecisionFixture("""
                do:
                  - unanimous:
                      call: decision
                      with:
                        task: decide
                        kind: boolean
                        input: '${ {text: .text} }'
                        options:
                          - true: { description: "Everyone agreed", then: approved }
                          - false: { description: "Someone disagreed", then: rejected }
                """ + OUTCOMES);
        assertThat(booleans.awaitCompleted(booleans.start("sim, todos")).get("outcome").asText()).isEqualTo("approved");
        assertThat(booleans.awaitCompleted(booleans.start("não")).get("outcome").asText()).isEqualTo("rejected");

        // A boolean without options and a score have one default case: the task's then, with the result as data.
        DecisionFixture scores = new DecisionFixture("""
                do:
                  - relevance:
                      call: decision
                      with:
                        task: decide
                        kind: score
                        input: '${ {text: .text} }'
                      output: { as: '${ {score: .value} }' }
                      then: scored
                  - skipped:
                      set: '${ {skipped: true} }'
                  - scored:
                      set: '${ . }'
                      export: { as: '${ $context + . }' }
                """);
        var context = scores.awaitCompleted(scores.start("12345"));
        assertThat(context.get("score").asInt()).isEqualTo(5);
        assertThat(context.has("skipped")).isFalse();
    }
}
