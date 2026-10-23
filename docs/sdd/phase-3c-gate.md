# Gate do PR da Fase 3c — checklist (preparado em 22/10/2026, PR **não aberto**)

PR da fase: "Fase 3c — Ponte entre domínios". Branch `phase-3c/domain-bridge` → `main`. Abrir o PR, fazer merge e marcar Validado ou Mergeado são decisões do Kayan.

## 1. SDDs da fase
| SDD | Título | Status | Pendência própria |
|---|---|---|---|
| 3c.1 | Ponteiro de grafo, nó de Referência externa e eventos enxutos | Implementado | — |
| 3c.2 | Resolvedor de ponteiros com política aplicada pela origem | Implementado | — |
| 3c.3 | Catálogo canônico de entidades como domínio | Implementado | — |
| 3c.3b | O nome de um termo do glossário não herda a sensibilidade da reunião | Implementado | — |
| 3c.4 | ADR rastreável às decisões de produto, com checagem de conflitos e revisão humana | Implementado | Modelo do ADR (o mínimo da 8h fica até você refinar); fio ponta a ponta do CA13 e o fim do CA7; opção `needs-change` na revisão |
| 3c.5 | Compliance da ponte: contrato de exposição, canal, restrito e trilha de auditoria | Implementado | **Parecer do DPO/jurídico e seu registro (CA4)** |
| 3c.6 | Marco: o cenário produto → arquitetura ponta a ponta, medido | Implementado | Execução do cenário com modelos reais (não ligada; ver 4) |

## 2. Verificações técnicas
| Item | Estado | Evidência |
|---|---|---|
| `mvn -o verify` do reator completo, com os ITs na floci | **Verde**: 879 testes, 0 falhas, 0 pulados (22/10/2026, 8 min 45 s) | seção 9.5 do 3c.6 |
| ArchUnit (cada regra com mais de zero classes) | **Verde**: 34/34 | `ArchitectureTest` |
| O cenário na floci (DynamoDB, SQS, S3, um worker) | **Verde** | `FlociScenarioIT`, `docs/sdd/evidence/3c.6-report-floci.json` |
| O cenário no processo, com o gabarito do corpus | **Verde** | `ScenarioRunTest`, `docs/sdd/evidence/3c.6-report.json` |
| Nenhum provedor real, nenhuma chave real | **Verde** | `NoRealProvidersTest` 6/6 |
| Wire só aditivo | **Verde**: `architecture@1.1.0` aditivo (`architecture@1` passa a resolver para ele); o resto da fase não mexe no wire | 3c.4 |
| Nativo do exemplo do marco | ver `docs/sdd/evidence/3c.6-native.txt` | 3c.6, seção 9.5 |

## 3. Gates não técnicos (bloqueiam dado real, não o merge do código)
| Item | Estado |
|---|---|
| **Parecer do DPO e do jurídico** sobre a ponte, registrado em `docs/compliance/bridge-review.md` (seção 5) e neste PR | **Pendente**, com o Kayan |
| Execução manual na **AWS** real: lifecycle do S3 e SSE-KMS (3a.5, seção 10) e a chave KMS por área (3b.5b, seção 10) | **Pendente**, com o Kayan |
| Pendência de **IP** (cláusula de invenção em horário ou equipamento) | **Pendente**, com o Kayan |
| Só dados sintéticos em todo o código, teste e evidência | **Verde** |

## 4. Achados abertos que pedem decisão ou SDD próprio
1. **Cenário com modelos reais não ligado.** O `ProductMeetingMain manual` cobre só a área de produto; a extração do conhecimento, o rascunho de arquitetura e a decisão de conflito do cenário usam os falsos. A medição de valor da POC (qualidade, custo e latência reais) pede isso.
2. **Atestação do canal é declaração de configuração** (3c.5, 3c.6): `registerResolve` agora a exige, mas autenticar o transporte e assinar o convite são do deployment.
3. **Trilha por instâncias, não por cadeia** (3c.5): `AuditLog` só lê por instância; "o que aconteceu com esta cadeia" em produção pede um índice por cadeia.
4. **Teste instável na primeira execução do `verify` do 3c.5** (`BridgeComplianceTest.ca3_ca9`, o fato `document-indexed` ainda não estava na lista): causa não provada; a hipótese é a ordem entre salvar a instância como terminada e publicar o fato. O cenário do 3c.6 espera o fato por sondagem. Se voltar, o motivo está nessa ordem.
5. **Modelo do ADR, painel do inspetor, fio ponta a ponta do 3c.4, opção `needs-change`** (3c.4): ver tabela 1.
6. **Pin do README do sandbox** (linha 30, de `500dfd1` para um commit novo): precisa da sua permissão, o sandbox é só leitura para o agente.
7. **Idioma:** a base guarda o idioma de origem (inglês); documentação de reunião em português pede um embedder multilíngue e um SDD próprio.
8. Itens herdados da Fase 3a e 3b que seguem abertos (ver `phase-3a-gate.md`, seção 4).

## 5. Validação externa (sandbox)
- O sandbox consome as fases como biblioteca. O gate da validação externa é do sandbox, depois do merge ou sobre o commit fixado. Nenhuma alteração do sandbox foi feita.

## 6. Estado da branch
- 238 commits à frente do `main` antes do commit do 3c.6. Os commits `833aca5`, `d5db1a2`, `0282940` e os do 3c.6 ainda não foram enviados. O push só com pedido explícito.

## 7. Itens para o texto do PR
- Resumo por SDD (tabela 1), números do build (2), gates pendentes (3) com o aviso **"nenhum dado real antes do parecer do DPO e do jurídico"**, achados (4) e os relatórios de métricas do cenário (`docs/sdd/evidence/3c.6-report*.json`), com a ressalva de que métricas de modelos falsos medem o encanamento.
