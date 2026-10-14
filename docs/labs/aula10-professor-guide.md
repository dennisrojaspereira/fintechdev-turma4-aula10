# Aula 10 — Guia do Professor: Game Day de Disaster Recovery

Gabarito e roteiro de condução do [Lab 24](24-game-day.md). **Não distribuir aos alunos.**

## Versão curta: demo guiada (recomendada)

Se o Game Day completo for demais para a turma, projete a demo. É uma história em 5 atos sobre **um único conceito, perda de dados no failover (RPO)**. Você aperta Enter entre os atos e a tela explica o que está acontecendo:

```bash
scripts/lab10.sh reset                 # ambiente limpo (~1 min)
scripts/lab10.sh demo                  # ~5 min; --atraso 30 = segundos de replicação cortada
```

Ou no navegador, melhor para projetar: `python3 scripts/aula10/painel10.py` e abra **http://localhost:8098/demo**. Avance com o botão ou com a barra de espaço. O botão "Reiniciar ambiente" (no início e no resumo) faz o `reset`.

1. **Tudo normal:** provider, banco principal e réplica crescem juntos (barras ao vivo).
2. **A réplica fica para trás:** a replicação é cortada e a barra da réplica congela.
3. **A região cai:** o principal sai do ar. Pergunta para a turma: esperar ou promover?
4. **Promover:** mostra a janela perdida (RPO observado) e quantos Pix o provider pagou que a Tech Pix não conhece mais.
5. **Reconciliar pelo provider:** os perdidos voltam a 0 e os clientes voltam.

A demo isola o antigo primary sozinha e pausa os clientes durante a queda, para não abrir as discussões de split-brain e duplicação. Para repetir: `reset`.

### Zonas e regiões (aba 2 da mesma página)

Em **http://localhost:8098/zonas** (ou pela aba "2 · Zonas e regiões"), a mesma infraestrutura vira uma topologia AWS: primary na `sa-east-1a`, standby **síncrono** na `sa-east-1b` (container `pg-standby`, que só sobe nesta demo) e réplica **assíncrona** em `us-east-1`. Ela cobre os slides 15, 18 e 21.

- **Cenário 1 · cai uma AZ:** failover para a `sa-east-1b`. Perda esperada: zero.
- **Cenário 2 · cai a região:** o enlace entre regiões cai, depois as duas AZs. Failover para `us-east-1`. Perda: o atraso da réplica, reconstruída depois pelo provider.

Rode um cenário, clique em "Reiniciar ambiente" e rode o outro. A tabela "Comparação" guarda os dois resultados lado a lado (fica em `lab10/.runtime/zonas-historico.json` e sobrevive ao reset).

## O que é real e o que é simulado

| Real (containers) | Simulado |
|---|---|
| PostgreSQL primary + réplica assíncrona por streaming (`pg_basebackup`, `pg_promote`) | o provider (PSP), em Python com SQLite |
| Kafka (KRaft), Outbox, relay, consumidor idempotente com offset manual | o "tráfego 2×" (12 clientes em vez de 6) |
| Perda de dados de verdade: a réplica para de receber WAL antes da queda | a "região": é a rede Docker `lab10-net` |
| Split-brain de verdade: o antigo primary volta e aceita escrita | — |
| Fencing de verdade: `docker network disconnect` | — |

O cenário bate com o slide 21: primary indisponível, réplica atrasada e provider ainda processando.

## Antes da aula

**Na véspera (20 min, sozinho):** ensaie o roteiro completo uma vez.

```bash
scripts/lab10.sh up
scripts/lab10.sh check                               # 7 x PASS: o "normal"
scripts/lab10.sh chaos start --lag 60 --volta 120    # em um terminal
# ... em outro terminal, siga o gabarito abaixo ...
scripts/lab10.sh report --equipe ensaio
scripts/lab10.sh reset
```

**Peça aos alunos que rodem `scripts/lab10.sh up` antes da aula.** O build baixa imagens e pacotes Python (~1 GB na primeira vez). O Wi-Fi da sala é o maior risco do Game Day. O `up` termina mostrando `ready: SIM`; o `check` deve dar 7 × `PASS`.

Requisitos por máquina: Docker com `docker compose` ou `docker-compose` (o controlador detecta os dois), Python 3.9+ e ~3 GB de RAM para o Docker (o Kafka roda com heap de 384 MB). Testado no Colima com 2 CPUs e 2 GB.

**Plano B:** uma equipe sem ambiente funcionando se junta a outra, ou você projeta o seu ambiente e a turma opera "por voz" (o comandante dita, você digita).

## Disparando o cenário

Cada equipe roda o lab **na própria máquina**, então cada uma dispara o próprio cenário. No minuto 100, todas ao mesmo tempo:

```bash
scripts/lab10.sh chaos start      # em um terminal dedicado; operar em OUTRO terminal
```

| Parâmetro | Padrão | Efeito |
|---|---|---|
| `--lag` | 90 s | a replicação é cortada na hora; a região cai LAG segundos depois. É a janela de perda (RPO observado) |
| `--volta` | 480 s | depois da queda, o antigo primary religa sozinho. É o momento do split-brain para quem não fez fencing. `0` desliga |

Com `--lag 90`, cada equipe perde em torno de 100–150 pagamentos que o provider liquidou (o número varia com a máquina). A volta em 8 min pega a maioria das equipes no meio da recuperação. Se a turma estiver rápida, use `--volta 300`.

`scripts/lab10.sh chaos primary-back` força a volta a qualquer momento (útil quando você projeta o seu ambiente).

## Cronograma (20 min)

```text
 0:00  chaos start em todas as maquinas; equipes distribuem papeis (lag de 90 s)
 1:30  queda da regiao
 1:30  detectar e declarar          (~3 min)
 4:30  conter e decidir             (~5 min)   <- pausa curta: "quem autorizou? com que evidencia?"
 9:30  recuperar                    (~6 min)   o antigo primary volta por volta de 9:30
15:30  reconciliar e validar        (~3 min)
18:30  reabrir + report             (~1,5 min)
```

Em 20 minutos nem toda equipe fecha. Uma equipe que não fechou e sabe dizer **por quê**, com evidência, também aprendeu. O relatório registra o que foi alcançado.

## Gabarito: o runbook que fecha

```bash
scripts/lab10.sh status                       # primary exited, kafka exited, replica PARADA ha ~90s
scripts/lab10.sh mark detectado
scripts/lab10.sh mark incidente-declarado <comandante>
scripts/lab10.sh maintenance on               # CONTER: clientes estao reenviando; nada de pagamento novo
scripts/lab10.sh fence                        # ANTES de promover: o antigo primary perde a autoridade
scripts/lab10.sh promote --autorizado-por <comandante>   # mostra a janela perdida
scripts/lab10.sh route replica
scripts/lab10.sh start kafka
scripts/lab10.sh restart worker               # a armadilha 2: ele leu o banco no boot
scripts/lab10.sh status                       # todos apontando para pg-replica? worker db ok, kafka ok?
scripts/lab10.sh reconcile                    # ler antes de aplicar
scripts/lab10.sh reconcile --apply
scripts/lab10.sh check                        # tudo PASS menos I7 (ainda em manutencao)
scripts/lab10.sh maintenance off              # REABRIR so agora
scripts/lab10.sh check                        # 7 x PASS
scripts/lab10.sh report --equipe <nome> --meta-rto <meta da turma>
```

Ensaiado, esse roteiro fecha em 1–2 minutos. Com discussão e decisão humana de verdade, 10–15. O `reconcile` típico mostra `AUSENTE_LOCAL` (pagamentos que o provider liquidou e o banco promovido não tem) e `LEDGER_ORFAO` (lançamentos de eventos consumidos antes da queda). O `--apply` reconstrói os pagamentos **com o mesmo id** a partir do provider; o evento republicado tem o mesmo `event_id` e o Ledger ignora como repetido (veja `repetidos ignorados` no `status`).

**Ordem aceitável alternativa:** promover antes do fencing também fecha, **desde que** o fencing aconteça antes de o antigo primary voltar. O lab mede o que aconteceu, não a ordem dos comandos. Na discussão, compare quem fez fencing antes e quem fez depois.

**Esperar o primary voltar em vez de promover** também é uma decisão válida: perda zero, RTO na mão do acaso. O `chaos start` religa o primary (`--volta`). Quem escolher esse caminho precisa de `fence replica`? Não: a réplica nunca foi promovida. O relatório mostra "sem promoção". Ótima discussão sobre RTO × RPO.

## As armadilhas (o que o lab provoca de verdade)

| # | Armadilha | Sintoma | Evidência no lab | Lição (slide) |
|---|---|---|---|---|
| 1 | `/health` responde `UP` com o banco fora | "está tudo verde" | `status`: `ready: NAO` | Running ≠ saudável (6) |
| 2 | o **worker** lê o banco só na inicialização | depois do `route`, pagamentos param em `PROCESSING` e o Ledger para de receber | `status`: `worker banco: pg-primary (lido no boot)`; `logs worker` | routing não muda quem já estava conectado (12) |
| 3 | reabrir antes de reconciliar | `I1 FAIL`: chaves liquidadas em dobro | `reconcile`: `DUPLICADO`; `logs loadgen`: "reenviando com a mesma chave" | recuperar → impedir duplicação → reconstruir → … (18) |
| 4 | não fazer fencing | quando o antigo primary volta, o worker (armadilha 2) escreve nele | `I6 FAIL`, `I6b WARN N escritas de split-brain` | fencing ≠ troca de routing (12) |
| 5 | rotear para a réplica antes de promover | API `ready: NAO (banco em modo leitura)` | `route` avisa | promover e rotear são passos distintos (11) |
| 6 | esquecer o Kafka | `I5 FAIL`: Outbox parado; Ledger sem lançamentos | `status`: outbox pendente cresce | brokers de volta ≠ efeitos recuperados (19) |

### Por que a armadilha 3 duplica (explique depois do Game Day)

A Payment API envia ao provider o **id interno** do pagamento como chave de idempotência, não a `Idempotency-Key` do cliente. Os clientes que tinham um Pix em andamento na hora da queda consultam o pagamento, recebem 404 (ele sumiu na janela do RPO) e reenviam com a mesma chave. Sem o registro, a Tech Pix gera um id novo → o provider vê uma transferência nova → o dinheiro sai duas vezes.

- **Durante o incidente**, a contenção resolve: com `maintenance on`, os reenvios recebem 503 até o `reconcile --apply` reconstruir os pagamentos. Aí o reenvio encontra o pagamento existente e nada duplica.
- **Depois do incidente**, a correção é de arquitetura: idempotência **ponta a ponta**, com a chave do cliente indo até o provider. É a resposta da pergunta 5 do Lab 24 e um ótimo item de "o que faríamos diferente".
- **Duplicação não se desfaz** com reconciliação. O lab põe as chaves em quarentena e o `check` mantém o I1 em `FAIL`: o próximo passo é estorno + contato com o cliente, decisões humanas.

## Critérios de sucesso (como avaliar o relatório)

| Critério (slide 21) | Onde está no relatório |
|---|---|
| Recuperação dentro da meta | "Recuperação validada" × "Meta de recuperação" |
| Operações afetadas identificadas | "Transações afetadas" + I3 `PASS` |
| Ausência de efeitos duplicados | "Efeitos financeiros duplicados: nenhum" + I1 `PASS` |
| Divergências resolvidas ou controladas | I2–I6 sem `FAIL`; `WARN`/quarentena explicados na seção `[preencher]` |

Pontos extras na discussão: a equipe justificou a promoção com o atraso medido da réplica? Registrou quem autorizou? Escreveu a mensagem para clientes e investidores?

## Discussão de fechamento (5 min, depois do Game Day)

1. **Projete dois relatórios:** um que fechou e um com duplicação. Mesma falha, mesmo ambiente: o que mudou foi a **ordem** das decisões.
2. **Perda observada × RPO do slide 9:** com uma réplica assíncrona e esse atraso, a meta da turma era realista? O que custaria reduzir (replicação síncrona, slide 11)?
3. **"Nunca offline":** o lab ficou indisponível para novos pagamentos durante a manutenção, **de propósito**. Indisponibilidade controlada foi melhor que disponibilidade com duplicação? É a resposta para os investidores (slide 23).

## IA no Game Day

As equipes podem usar IA para interpretar a saída do `reconcile`, levantar hipóteses ("por que o Ledger parou?") e escrever consultas `sql`. Siga a regra da aula: **recomendação → hipótese → experimento → evidência → decisão humana**. O `promote` exige `--autorizado-por` com o nome de uma **pessoa**: a IA pode recomendar o failover, não autorizar.

## Referência rápida

```bash
scripts/lab10.sh status
scripts/lab10.sh logs worker 20
scripts/lab10.sh sql replica "SELECT status, count(*) FROM payments GROUP BY 1"
scripts/lab10.sh sql primary "SELECT count(*) FROM payments WHERE updated_at > now() - interval '5 min'"
scripts/lab10.sh sql ledger "SELECT count(*) FROM processed_events"
scripts/lab10.sh provider <payment_id>
```

Arquivos de estado: `lab10/.runtime/` (roteamento, manutenção, linha do tempo em `state.json`). Relatórios: `relatorios/`. Ambos ficam fora do git.
