# Tech Pix — Do Monólito ao Sistema Distribuído

Tech Pix começou como um monólito.

E isso não era um problema.

O sistema atendia 10 TPS com baixa latência. Cinco módulos, um processo, um banco.

Depois a empresa cresceu. Fraud ficou mais sofisticado. A latência aumentou. O banco começou a saturar.

A partir daqui investigamos o problema antes de mudar a arquitetura.

> **Distribuir é consequência, não objetivo.**

Este repositório é um laboratório executável da Aula 07. Cada etapa da arquitetura existe porque um problema foi medido na etapa anterior. O código conta a história; os documentos em `docs/labs/` explicam cada passo; as tags git congelam cada momento.

```text
Monolith
    |  performance evidence           labs 02-03
Modular Monolith                      labs 04-05
    |  operational difference         lab 06
Fraud Extraction                      labs 07-08
    |  independent runtime            labs 09-12
Kubernetes
    |  safe migration                 labs 13-15
Parallel Run / Canary
    |  environment complexity         labs 16-17
GitOps / Argo CD
    |  data ownership                 lab 18
Database per Service
    |  need for shared facts          lab 19
Kafka
    |
Distributed System Problems           lab 20
```

## Como executar

Pré-requisitos por nível. Cada nível inclui o anterior.

| Nível | Precisa de | Cobre |
|---|---|---|
| 1 | Java 21, Docker | labs 01 a 08, 13 a 15, 18 a 20, todos os testes |
| 2 | + k6 | testes de carga (labs 02 a 06) |
| 3 | + kind, kubectl | labs 09 a 12, 16 |
| 4 | + tempo para instalar o Argo CD | lab 17 |

### Nível 1

```bash
docker compose up -d postgres
./mvnw test                                  # 69 testes; Testcontainers sobe PostgreSQL e Kafka
./mvnw -pl monolith spring-boot:run          # http://localhost:8080
scripts/demo-payment.sh
```

A stack completa em containers (PostgreSQL, Kafka, monólito, Fraud Service):

```bash
docker compose --profile app up -d --build
scripts/fraud-mode.sh NEW
scripts/demo-payment.sh
docker compose logs fraud-service | grep aplicado
```

### Nível 2

```bash
scripts/seed.sh                              # 2.000 contas, 200.000 pagamentos
scripts/fraud-profile.sh HEAVY
scripts/load-test.sh growth
scripts/show-metrics.sh
scripts/slow-queries.sh
```

### Nível 3

```bash
scripts/k8s-up.sh                            # cluster kind "techpix", imagens locais, manifests
scripts/k8s-discovery-demo.sh
scripts/k8s-scale.sh 2 5
scripts/k8s-up.sh gitops dev                 # overlays por ambiente
scripts/k8s-down.sh
```

### Nível 4

```bash
scripts/local-git-server.sh                  # Gitea em container: Argo CD sem GitHub
TECHPIX_GIT_URL=http://host.docker.internal:3001/techpix/tech-pix.git scripts/argocd-up.sh
scripts/argocd-drift-demo.sh
```

### Aula 8 — Segurança (pipeline SAST/DAST e autenticação)

Pipeline de segurança com Tekton no cluster kind: clone (Gitea local) → SAST (SonarQube com quality gate) → DAST (OWASP ZAP contra a aplicação rodando):

```bash
scripts/k8s-up.sh                            # alvo do DAST
scripts/local-git-server.sh                  # codigo no Gitea para o clone
scripts/tekton-up.sh                         # Tekton + SonarQube (localhost:9000) + manifests tekton/
scripts/pipeline-run.sh                      # dispara e acompanha; SAST em http://localhost:9000
```

Autenticação com Keycloak (realm, clients e usuários pré-provisionados) e tela de login:

```bash
scripts/keycloak-up.sh                       # Keycloak em :8180 (admin/admin), realm techpix importado
docker compose up -d postgres
SPRING_PROFILES_ACTIVE=secure ./mvnw -pl monolith spring-boot:run
# http://localhost:8080/login -> maria/techpix123 (admin) ou joao/techpix123 (user)
```

Sem o profile `secure`, nada muda: os labs anteriores e os testes continuam funcionando sem Keycloak.

O realm agora traz usuários realistas (maria/admin, ana.lima/suporte, beatriz.rocha/compliance, carlos.andrade e joao/clientes — senha `techpix123`) e os dois serviços como clients confidenciais com service account. A demonstração de **serviço falando com serviço** (client credentials do monolito → introspection no Fraud Service → autorizado pela role `fraud-evaluate`; token de usuário → negado):

```bash
scripts/keycloak-m2m-demo.sh
```

**Policy as code com OPA**: um servidor de decisão ([docker/opa/policies/techpix.rego](docker/opa/policies/techpix.rego)) responde "posso?" para os serviços — limites de Pix por papel, Pix alto exige aprovação de compliance, chamada serviço-a-serviço exige role de serviço. A demo usa as roles de tokens reais do Keycloak como input (identidade diz *quem* você é; política diz *o que* pode), e a política pode ser editada ao vivo, sem redeploy:

```bash
docker compose --profile auth up -d opa        # OPA em :8181
scripts/opa-demo.sh                            # 7 decisoes: ALLOW/DENY com motivos
```

A pipeline também publica os artefatos aprovados num **JFrog Artifactory** local, e dá para encenar o ciclo completo de CI (commit → gates → publicação) sem tocar no seu repositório — os commits simulados acontecem num clone temporário do Gitea:

```bash
scripts/jfrog-up.sh                          # Artifactory OSS em :8082 (admin/password)
scripts/simulate-commit.sh                   # commit inofensivo -> SAST -> DAST -> publish
scripts/simulate-commit.sh vulneravel        # senha hardcoded + DES/MD5 -> gate REPROVA
scripts/simulate-commit.sh corrigir          # remove a vulnerabilidade -> volta a passar
```

**Synthetic monitoring**: um robô k6 ([load-tests/synthetic-pix.js](load-tests/synthetic-pix.js)) refaz a jornada completa do Pix — abre contas, paga, confere `APPROVED` **e o crédito na conta do recebedor** — de tempos em tempos, e o resultado vira métrica no Prometheus (painel "Synthetic" na Visão Geral do Grafana). É o lado **proativo** da observabilidade: às 3h da manhã, sem tráfego, é ele quem descobre que o Pix quebrou. Sem k6 instalado, a mesma jornada roda em Python puro.

```bash
python scripts/synthetic-monitor.py run      # testa a operacao AGORA (veredicto no console)
python scripts/synthetic-monitor.py start    # monitor em background, a cada 2 min
python scripts/synthetic-monitor.py status   # historico das ultimas execucoes
```

### Subir tudo com um comando (sem .sh)

Depois de provisionar uma vez (cluster kind, Gitea, Tekton, JFrog), qualquer reboot se resolve com:

```bash
python scripts/sobe-tudo.py                  # Docker, compose, kind, port-forwards, lab 9, painel
```

### Dashboards do Grafana

`http://localhost:3000` abre a **Visão Geral** (lista de todos os dashboards + status dos targets). Pastas em `/dashboards`: **Aula 7** (Payment vs Fraud), **Serviços — RED & Golden Signals** (Monolith e Fraud Service: Rate/Errors/Duration, pool HikariCP, eventos e idempotência, CPU/heap) e **Aula 9 — Observabilidade** (o incidente "Cadê o Pix?" reencenado em loop de 20 min, com os conceitos da aula nos painéis, e o **"Trace de um Pix"**: cole um `trace_id` e explore o waterfall de spans no Grafana Tempo, ingerido via OTLP).

### Aula 9 — Observabilidade e Resiliência (laboratório final "Cadê o Pix?")

Investigação de incidente: o sistema está "todo verde", mas um Pix de R$ 250,00 saiu da conta e não chegou ao destinatário. O aluno segue logs estruturados, correlation ID, trace, métricas (RED/USE/Golden Signals), estado UNKNOWN, retry vs idempotência, reconciliação, DLQ, SLI/SLO, MTTD/MTTR, RCA e IA — tudo dentro do mesmo incidente. Roda sem cluster, sem Kafka e sem o monólito (simulador autocontido em Python):

```bash
scripts/lab9-up.sh                           # "produção" do lab em :8080
curl localhost:8080/actuator/health          # está tudo verde... será?
scripts/investigate-logs.sh PIX-928371       # a investigação começa aqui
scripts/lab9-incident2.sh                    # desafio final (sem gabarito)
scripts/lab9-up.sh stop
```

Roteiro do aluno: [Lab 23 — Cadê o Pix?](docs/labs/23-cade-o-pix.md) · Entregável: [Incident Report](docs/labs/aula09-incident-report-template.md) · Gabarito: [guia do professor](docs/labs/aula09-professor-guide.md) (não distribuir).

### Aula 10 — Continuidade e Disaster Recovery (Game Day)

Os investidores querem que a Tech Pix "nunca fique fora do ar". O Game Day mostra o que isso custa: a região principal cai às 18h de uma sexta, a réplica está atrasada e o provider continua liquidando Pix. Cada equipe precisa **decidir, recuperar, reconciliar e validar** sem perder nem duplicar dinheiro, e o laboratório **mede** detecção, recuperação, perda observada, duplicações e invariantes.

Desta vez a infraestrutura é real: PostgreSQL primary + réplica por streaming, Kafka com Outbox e consumidor idempotente, split-brain e fencing de verdade. Só o provider é simulado. Stack própria (`docker-compose.lab10.yml`, portas 8088 e 8090), independente das aulas 7–9; precisa de Docker e Python 3.9+, sem cluster.

```bash
scripts/lab10.sh up                          # antes da aula: build + ambiente saudavel
scripts/lab10.sh check                       # 7 invariantes em PASS: o "normal"
scripts/lab10.sh chaos start                 # o desastre (terminal dedicado)
scripts/lab10.sh status                      # ... e a equipe assume: promote, fence, route, reconcile, check
scripts/lab10.sh report --equipe azul        # relatorio com as medicoes
scripts/lab10.sh reset                       # entre turmas
```

Roteiro do aluno: [Lab 24 — Game Day](docs/labs/24-game-day.md) · Gabarito: [guia do professor](docs/labs/aula10-professor-guide.md) (não distribuir).

### Roteiro de apresentação — Aulas 8 e 9

Antes da aula (10 min, sozinho): `python scripts/sobe-tudo.py` (deve terminar 11/11 UP), abas abertas em painel (8099), Grafana (3000), SonarQube (9000), JFrog (8082), Keycloak (8180) e Tekton (9097), e um `scripts/simulate-commit.sh` de aquecimento para o cache Maven (a pipeline cai de ~5 para ~2 min).

**Aula 8 — Segurança no ciclo de entrega (~45 min)**, tudo pelos botões do grupo "Segurança" do painel:

| # | Momento | Como mostrar |
|---|---|---|
| 1 | O fluxo feliz (10 min) | "Imitar commit + pipeline + publicar no JFrog". Enquanto roda, o commit no Gitea e a execução no Tekton Dashboard. Ao final, "Artefatos publicados no JFrog": o jar versionado pelo SHA. *Artefato publicado = artefato que passou pelos gates.* |
| 2 | O commit de sexta-feira (15 min) | "Imitar commit VULNERÁVEL". Antes, mostre o `LegacyPixExporter.java` injetado: compila, parece inofensivo, tem até um `TODO: mover para o vault`. A pipeline reprova no SAST — percorra as violações no SonarQube (senha hardcoded, DES/ECB, MD5, Random). Pergunta: *quantas dessas um code review humano pegaria às 18h de sexta?* DAST e publish nem rodam: nada vulnerável chega ao repositório de artefatos. |
| 3 | O conserto (5 min) | "Corrigir a vulnerabilidade + pipeline": gate volta a OK, novo artefato no JFrog. O ciclo commit → gate → publicação, duas vezes, com desfechos opostos. |
| 4 | Identidade: gente e serviços (10 min) | `scripts/keycloak-m2m-demo.sh`: token de usuário vs token de serviço (compare os claims), introspection autorizando o monolito pela role `fraud-evaluate` e negando o usuário, e a chamada real ao Fraud. *Autenticar ≠ autorizar; serviço também tem identidade.* |
| 5 | Política como código (10 min) | Botão "OPA: 7 decisões de política": limites por papel, Pix de R$ 25.000 negado sem compliance e aprovado com, serviço vs usuário no `fraud.evaluate` — tudo com roles de tokens reais. Edite o limite no `techpix.rego`, `docker restart techpix-opa`, rode de novo: **a regra mudou sem redeploy**. |

Gancho para a Aula 9: "a pipeline garante o que **entra** em produção. E quando o que está em produção, todo verde, perde um Pix de R$ 250?"

**Aula 9 — Observabilidade (~50 min):**

| # | Momento | Como mostrar |
|---|---|---|
| 1 | Teoria com dashboard vivo (10 min) | Nos slides de RED/USE/Golden Signals, deixe projetado Grafana → "Serviços — RED & Golden Signals" com um k6 leve rodando. Opcional: `scripts/chaos.sh latency 800` ao vivo — Duration sobe, Errors não. Cardinalidade: Visão Geral → "por que não existe painel do PIX-928371?". |
| 2 | Lab "Cadê o Pix?" (30–40 min) | [Lab 23](docs/labs/23-cade-o-pix.md) com o [guia do professor](docs/labs/aula09-professor-guide.md) aberto só para você; espelhe pelos botões "Aula 9" do painel. Na Fase 5, abra o dashboard "Aula 9 — Cadê o Pix?": o incidente reencena em loop de 20 min — cronometrando o início do lab com o ciclo, o deploy v1.13.4 acontece ao vivo. Pausas obrigatórias: a votação do retry (Fase 8) e o "onde estava o Pix?" (Fase 10). |
| 3 | Desafio final (para casa ou +15 min) | "Ativar o desafio final": novo chamado, sem gabarito. Entregável: o Incident Report do template. |

Fechamento que amarra as duas aulas: a Aula 8 responde *"como impedir que o problema entre?"* (gates antes da produção); a Aula 9 responde *"como explicar o que acontece lá dentro?"* (evidência depois do deploy) — e o vilão do incidente 1 é justamente um deploy que passou nos gates de segurança. **Pipeline não substitui observabilidade.**

Todos os acessos (UIs, portas, credenciais, comandos) em um lugar: abra [hub.html](hub.html) no navegador.

No Windows, cada script em `scripts/` tem um equivalente `.ps1`; os scripts `.sh` também rodam no Git Bash.

## Labs

Cada lab segue a mesma estrutura: problema, o que observar, hipóteses, mudança, como executar, como testar, resultado, trade-offs, pergunta para discussão, Professor Notes, próximo problema. Cada um informa o slide que suporta e a tag que congela aquele momento.

| Lab | Slides | Tag |
|---|---|---|
| [01 — O monólito que funciona](docs/labs/01-working-monolith.md) | 1 | `aula07-step-01-monolith` |
| [02 — O crescimento muda o problema](docs/labs/02-growing-load.md) | 2, 3 | `aula07-step-02-growth` |
| [03 — Sintoma não é causa](docs/labs/03-investigating-performance.md) | 4, 5 | `aula07-step-03-observability` |
| [04 — Otimizar antes de distribuir](docs/labs/04-optimizing-fraud.md) | 6 | `aula07-step-04-optimized` |
| [05 — Modular Monolith](docs/labs/05-modular-monolith.md) | 7 | `aula07-step-05-modular` |
| [06 — A dor persiste: decidir extrair](docs/labs/06-extraction-decision.md) | 8, 9, 10 | `aula07-step-06-strangler` |
| [07 — Strangler Fig](docs/labs/07-strangler.md) | 11 | `aula07-step-06-strangler` |
| [08 — Self-contained Fraud Service + ACL](docs/labs/08-fraud-service.md) | 12 | `aula07-step-07-fraud-service` |
| [09 — Onde Fraud roda? Containers e Kubernetes](docs/labs/09-kubernetes.md) | 13, 14 | `aula07-step-08-kubernetes` |
| [10 — Service Discovery](docs/labs/10-service-discovery.md) | 15 | `aula07-step-08-kubernetes` |
| [11 — Escala independente](docs/labs/11-scaling.md) | 16 | `aula07-step-08-kubernetes` |
| [12 — Readiness, Liveness e Startup](docs/labs/12-health-checks.md) | 17 | `aula07-step-08-kubernetes` |
| [13 — Branch by Abstraction + Feature Flag](docs/labs/13-feature-flags.md) | 18 | `aula07-step-09-feature-flag-parallel-run` |
| [14 — Parallel Run](docs/labs/14-parallel-run.md) | 19 | `aula07-step-09-feature-flag-parallel-run` |
| [15 — Canary](docs/labs/15-canary.md) | 20 | `aula07-step-10-canary` |
| [16 — Configuração dos ambientes e GitOps](docs/labs/16-gitops.md) | 21 | `aula07-step-11-gitops-argocd` |
| [17 — Argo CD: reconciliação](docs/labs/17-argocd.md) | 22 | `aula07-step-11-gitops-argocd` |
| [18 — Shared Database e Database per Service](docs/labs/18-database-per-service.md) | 23, 24 | `aula07-step-12-database-per-service` |
| [19 — Kafka: o JOIN virou evento](docs/labs/19-kafka.md) | 25 | `aula07-step-13-kafka` |
| [20 — Complexidade dos sistemas distribuídos](docs/labs/20-distributed-systems.md) | 26 | `aula07-step-14-distributed-problems` |
| [21 — Pipeline de segurança: Tekton, SAST e DAST](docs/labs/21-secure-pipeline.md) | Aula 8 | — |
| [22 — Autenticação com Keycloak e tela de login](docs/labs/22-keycloak-login.md) | Aula 8 | — |
| [23 — Cadê o Pix? Observabilidade e resiliência](docs/labs/23-cade-o-pix.md) | Aula 9 | — |
| [24 — Game Day: a região principal caiu](docs/labs/24-game-day.md) | Aula 10 | — |

O [mapa dos 26 slides](docs/architecture/slides-map.md) diz o que demonstrar ao vivo em cada um e propõe um roteiro de 2 horas. A [evolução dos diagramas](docs/architecture/evolution.md) mostra a arquitetura mudando, lab a lab.

## Navegar pela história

```bash
scripts/step.sh              # lista as 14 tags
scripts/step.sh 4            # checkout da etapa 4 (monolito otimizado)
scripts/step.sh diff 3 4     # o que mudou
scripts/step.sh main         # de volta ao fim
```

O código em `main` preserva todos os caminhos anteriores por configuração: `techpix.fraud.profile=SIMPLE|HEAVY|HEAVY_OPTIMIZED` reproduz as etapas 1, 2 e 4; `techpix.fraud.mode=LEGACY|PARALLEL|CANARY|NEW` reproduz 5, 9, 10 e 8; o Fraud Service em modo banco compartilhado reproduz a etapa 8 (ver lab 18). Dá para apresentar a aula inteira sem trocar de tag.

## Estrutura

```text
tech-pix/
├── monolith/            Account, Payment, Ledger, Fraud (legado + facade), Notification
├── fraud-service/       o serviço extraído: API, domínio com portas, ACL, visão local, consumer Kafka
├── load-tests/          k6
├── docker/              init do PostgreSQL, Prometheus, Grafana
├── kubernetes/          kind-config e os manifests "crus" dos labs 09 a 12 (apontam para gitops/base)
├── lab10/               Game Day da Aula 10: servicos (payment, worker, ledger, provider, clientes) e init dos bancos
├── gitops/              base + overlays dev/qa/prod (Kustomize)
├── argocd/              Applications
├── docs/
│   ├── labs/            24 laboratórios
│   ├── adr/             8 decisões, com consequências negativas
│   └── architecture/    fronteiras, decision framework, evolução dos diagramas, mapa dos slides
├── scripts/             tudo que o instrutor roda ao vivo (.sh e .ps1)
├── docker-compose.yml
└── docker-compose.lab10.yml   stack do Game Day (primary + replica, Kafka, provider)
```

## Decisões

| ADR | Decisão |
|---|---|
| [001](docs/adr/001-keep-monolith.md) | Manter o monólito |
| [002](docs/adr/002-modularize-fraud.md) | Modularizar, com Fraud como Business Capability |
| [003](docs/adr/003-extract-fraud-service.md) | Extrair Fraud como serviço, incrementalmente |
| [004](docs/adr/004-use-kubernetes.md) | Executar o Fraud Service em Kubernetes |
| [005](docs/adr/005-use-gitops.md) | GitOps com Kustomize e Argo CD |
| [006](docs/adr/006-fraud-data-ownership.md) | Ownership de dados: Fraud Store separado |
| [007](docs/adr/007-use-kafka-for-payment-facts.md) | Kafka para os fatos de Payment |
| [008](docs/adr/008-saga-compensation.md) | Compensação (Saga mínima) para a liquidação |

Cada ADR tem contexto, decisão, alternativas e consequências. Principalmente as negativas.

## Testes

```bash
./mvnw test
```

| Tipo | Onde |
|---|---|
| unitários | regras de fraude, `RiskEngine`, `CanaryRouter`, `RetryPolicy` |
| integração com PostgreSQL real | fluxo de pagamento, perfis, otimização, contagem de consultas |
| arquitetura | `ModuleBoundaryTest` (monólito), `DomainIsolationTest` (Fraud Service) |
| Strangler, feature flag, parallel run, canary, timeout e retry | contra um Fraud Service falso (`HttpServer` do JDK) |
| Fraud Service | domínio puro, schema legado (etapa 8), banco próprio (etapa 12), health probes |
| Kafka real | produtor, consumidor, idempotência, compensação |

Testcontainers exige Docker rodando. Nada exige Kubernetes ou Argo CD.

## Números medidos nesta máquina

Os valores absolutos dependem do notebook. As proporções são o que importa.

| Momento | Throughput | p95 | Consultas por pagamento |
|---|---|---|---|
| Fraud com 17 regras, sem índices (lab 02) | 9 TPS | 6,9 s | 32 |
| Só índices e transação curta (lab 04) | 133 TPS | 326 ms | 34 |
| Consultas agregadas e cache (lab 04) | 193 TPS | 213 ms | 16 |

E a evidência para a extração (lab 06): dobrar o custo de CPU de Fraud deixou uma rota sem Fraud 8x mais lenta.

## A pergunta final

> Por que essa arquitetura existe?

Se a resposta for "porque microserviços escalam", a aula falhou. Se a resposta for a sequência de problemas medidos acima, funcionou.
