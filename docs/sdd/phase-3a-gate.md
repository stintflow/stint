# Gate do PR da Fase 3a — checklist (preparado em 15/10/2026, PR **não aberto**)

PR da fase: "Fase 3a — IA: decisão, worker LangChain4j e humano no loop". Branch `phase-3a/ai-decision-workers` → `main`. Abrir o PR, fazer merge e marcar Validado ou Mergeado são decisões do Kayan.

## 1. SDDs da fase
| SDD | Título | Status | Pendência própria |
|---|---|---|---|
| 3a.1 | `call: decision` | Implementado | — |
| 3a.2 | Adapters de decisão (LLM, Jev, política) | Implementado | — |
| 3a.2b | Perfil local LitJev | Implementado | Calibração por correções (ver 3a.4, fora do escopo) |
| 3a.3 | Worker LangChain4j | Implementado | — |
| 3a.3b | Ordem da memória | Implementado | — |
| 3a.3c | Prazos e tentativas superadas | Implementado | — |
| 3a.4 | Revisão humana via `listen` e correções | Implementado | Revisar 9.3 (decisões tomadas sem escolha explícita); correção do despacho feita no 3a.6 |
| 3a.5 | Compliance de entrada | Implementado | Revisar 9.4 (entrada bruta em repouso pela vida da instância) |
| 3a.6 | Marco: reunião ponta a ponta | Implementado | Achado grave de PII falada (9.5) para o S.6 |
| 3a.7 | Binding SQS do worker | Implementado | — |

## 2. Verificações técnicas
| Item | Estado | Evidência |
|---|---|---|
| `mvn install` completo, offline, com ITs do floci | **Verde**: 516 testes, 0 falhas (15/10/2026) | seção 9 do 3a.6 |
| ArchUnit (cada regra com mais de zero classes) | **Verde**: 16/16 | `ArchitectureTest` |
| Nenhum provedor real, nenhuma chave real | **Verde** | `NoRealProvidersTest` 6/6 |
| Nativo do exemplo de IA | **Verde** (3a.4, com o canal HTTP) | `docs/sdd/evidence/3a.4-native.txt` |
| Nativo do marco | **Verde** | `docs/sdd/evidence/3a.6-native.txt` |
| Nativo do smoke AWS | **Verde** (3a.7) | `docs/sdd/evidence/3a.7-smoke-native.txt` |
| Execução manual com modelos locais | **Verde** (3a.2b CA6, 3a.6 CA2) | `3a.2b-CA6-litjev.txt`, `3a.6-CA2-manual.txt` |
| Wire só aditivo | **Verde**: `stintsensitivity`, `stintprovider`, `stintrole`, tipos `io.stintflow.review.*`, todos opcionais na leitura | 3a.4, 3a.5 |

## 3. Gates não técnicos (bloqueiam dado real, não o merge do código)
| Item | Estado |
|---|---|
| Validação do desenho com **DPO e jurídico** (LGPD), registrada no 3a.5 (seção 10) e neste PR | **Pendente** |
| Execução manual na **AWS** real: efeito do lifecycle do S3 e SSE-KMS | **Pendente** |
| Pendência de **IP** (cláusula de invenção em horário ou equipamento) | **Pendente**, com o Kayan |
| Só dados sintéticos em todo o código, teste e evidência | **Verde** |

## 4. Achados abertos que pedem decisão ou SDD próprio
1. **PII falada** escapa do redator de referência (3a.6, 9.5): camada para a forma falada no S.6 antes de qualquer áudio real.
2. Correlação de `listen` por `.data` de fato restrito, e `read: envelope/raw` com `dataref` (3a.5, 9.5).
3. Inbox de eventos adiantados inline fora do caso de revisão (3a.5, 9.5).
4. Resposta adiantada de outro papel aceita se o escalonamento vier dentro da janela do inbox (3a.4, 9.4).
5. Bucket de fatos sem SSE (3a.5, 9.5); auditoria fora da transação (3a.5).
6. Calibração offline a partir das correções (3a.4, 4.7): SDD próprio.
7. `SqsPollLoop` do engine serial (3a.7, 9.5).

## 5. Validação externa (sandbox)
- S.5 a S.11 consomem esta fase como biblioteca (portas, contratos, corpus e pilha local entregues no 3a.6). O gate da validação externa 3a é do sandbox, depois do merge ou sobre o commit fixado.

## 6. Estado da branch
- 12 commits locais depois do último push (do 3a.6), e 200 commits à frente do `main`. O push só com pedido explícito.

## 7. Itens para o texto do PR
- Resumo por SDD (tabela 1), números do build (2), gates pendentes (3) com o aviso **"nenhum dado real antes da validação com DPO e jurídico"**, achados (4).
