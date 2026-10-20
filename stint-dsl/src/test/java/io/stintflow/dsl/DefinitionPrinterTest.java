package io.stintflow.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;

/** SDD 3a.1, CA8 (RF8): what a {@code call: decision} expands to, printed back as DSL 1.0 YAML. */
class DefinitionPrinterTest {

    @Test
    void a_decision_prints_as_a_remote_call_and_a_switch() {
        WorkflowDefinition def = new DslLoader().load(new ByteArrayInputStream("""
                document:
                  dsl: '1.0.0'
                  namespace: test
                  name: printed
                  version: '1.0.0'
                do:
                  - classify:
                      call: decision
                      with:
                        task: decide
                        kind: choice
                        input: '${ {text: .text} }'
                        options:
                          - approve: { description: "Approved", then: approved }
                          - reject: {}
                      onUncertain: review
                      timeout: { after: { seconds: 60 } }
                      export: { as: '${ {decided: .value} }' }
                """.getBytes(StandardCharsets.UTF_8)), "printed.yaml", LoadOptions.STRICT);

        assertThat(DefinitionPrinter.print(def)).isEqualTo("""
                document:
                  dsl: 1.0.0
                  namespace: test
                  name: printed
                  version: 1.0.0
                do:
                - classify:
                    call: remote
                    with:
                      task: decide
                      input: "${ {\\"kind\\": \\"choice\\", \\"options\\": [{\\"name\\":\\"approve\\",\\"description\\":\\"Approved\\"},{\\"name\\":\\"reject\\"}], \\"input\\": (({text: .text}))} }"
                    timeout:
                      after: PT1M
                    x-stint-output-contract: Decision output out of contract
                - classify/route:
                    switch:
                    - uncertain:
                        when: "${ .uncertain == true }"
                        then: review
                    - approve:
                        when: "${ .value == \\"approve\\" }"
                        then: approved
                    - reject:
                        when: "${ .value == \\"reject\\" }"
                        then: continue
                    export:
                      as: "${ {decided: .value} }"
                """);
    }
}
