# Ponte entre domínios — material para o parecer do DPO e do jurídico

> **Isto não é um parecer.** É o desenho técnico da ponte (SDD 3c.1 a 3c.5), o que cada controle garante e não garante, e as perguntas que só o DPO e o jurídico respondem. Enquanto o registro da seção 5 não existir, a ponte roda **só com dados sintéticos** (gate da seção 10 do SDD 3a.5).

## 1. O que a ponte faz
Dois ou mais domínios (áreas) guardam cada um o seu grafo de conhecimento. A ponte deixa um domínio **apontar** para um nó de outro (um ponteiro), **resolver** o ponteiro (receber, por um instante, a visão que a origem publica) e **cruzar** o que cada um diz do mesmo sistema (o catálogo). Nenhum domínio copia o grafo do outro. O conteúdo resolvido vive num blob de 1 dia, fora do estado da instância.

## 2. Controles técnicos, o que garantem e o que não garantem
| Controle | Onde | Garante | **Não** garante |
|---|---|---|---|
| Contrato de exposição versionado (`BridgeContract`, SDD 3c.5) | o worker da origem | quem (definição), o quê (tipos e propriedades por visão) e até que nível (nunca acima de `confidential`) o domínio expõe; a versão e o hash do contrato vão na auditoria de cada resolução | que a definição que pede é quem diz ser (ver canal) |
| Restrito fora da ponte | `Grant` (recusa `restricted`), `knowledge-announce`, `BridgeContract` | nenhum nó restrito é anunciado nem resolvido; o pedido recebe a mesma resposta de qualquer nó negado | que um nó mal classificado como `confidential` não vaze: a classificação é da extração (3b.1/3b.2) |
| Negação idêntica | `resolve-pointers` | quem não pode saber não distingue "negado" de "não existe" | |
| Propriedade sensível só com `allowSensitive` | `PublishedView` | o `summary` de uma decisão só chega à definição que o contrato lista, e a auditoria registra `allowSensitive` | que o adapter de decisão (local no build) trate o texto com o cuidado devido: é provedor aprovado por nível (3a.5) |
| Atestação do canal (`ChannelAttestation`) | `ResolvePointersHandler` | falha fechada: com a atestação "não autenticado", a origem não responde; a auditoria diz por qual canal | **que o canal seja de fato autenticado**: é uma declaração de configuração. Autenticar o transporte (IAM, mTLS) é do deployment; o convite não é assinado |
| Vida curta do blob resolvido | `ResolvedViews` | 1 dia no mínimo da plataforma, descartável após o uso | que ninguém leia nesse dia: quem tem acesso ao armazenamento do blob lê |
| Trilha de auditoria (`AuditTrail`) | auditoria dos workers e do motor | ingestão, anúncio, vínculo, resolução, catálogo, ADR e publicação registram ids, níveis e contagens; nunca texto | que a auditoria seja imutável ou tenha a retenção que a lei exige: é a configuração do armazém |
| Dados sintéticos | testes e exemplos | nenhum dado real nos testes | |

## 3. Perguntas ao DPO e ao jurídico
1. **Base legal** para uma área ver uma decisão de outra (a "motivada por" do ADR): qual base da LGPD sustenta o compartilhamento entre áreas da mesma organização? Muda se a área for outra entidade?
2. **Finalidade e minimização:** o contrato lista tipos e propriedades por consumidor. Esse nível de granularidade basta? O `summary` de uma decisão (que só o `architecture-adr` recebe) é aceitável como dado compartilhado?
3. **Retenção do blob resolvido (1 dia) e da auditoria:** os prazos são adequados? A auditoria deve ter retenção própria e imutabilidade?
4. **O revisor humano:** o material do par (texto das duas decisões) é apresentado ao `tech-rep`. O papel, o registro do revisor (pseudonimizado) e o prazo estão adequados?
5. **Direito do titular quando o dado vive em dois domínios:** a eliminação (SDD 3b.5) apaga o nó na origem; o ponteiro em outro domínio passa a `REMOVED`. Isso atende ao direito de eliminação, ou o domínio que apontou também precisa apagar a Referência?
6. **Restrito:** a posição atual é "nunca cruza". Existe caso de uso que justifique uma exceção, com que aprovação e por quanto tempo?
7. **Canal:** o que o deployment real deve exigir (IAM, mTLS, assinatura) para a atestação ser aceita como controle?
8. **Região e provedores:** em que região ficam os armazéns, os blobs e os provedores de modelo, por nível?
9. **O catálogo:** guardar nomes de sistemas, produtos e equipes (nunca pessoas) é dado pessoal? Ele fica `internal` para todos os domínios.

## 4. O que o código não resolve (e precisa de decisão fora dele)
- Autenticação e criptografia do transporte entre domínios reais.
- Retenção e imutabilidade do armazém de auditoria.
- A classificação correta dos nós na origem.
- A execução manual na AWS (lifecycle e SSE-KMS) do SDD 3a.5.

## 5. Registro da validação (a preencher)
| Campo | Valor |
|---|---|
| Data | |
| Quem validou (DPO / jurídico) | |
| Escopo validado (SDDs, versão do contrato, commit) | |
| Ressalvas | |
| Anexo na Biblioteca / no PR da fase 3c | |
