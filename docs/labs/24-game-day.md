# Lab 24 — Game Day: a região principal caiu (Aula 10)

**Aula:** 10 — Continuidade, Disaster Recovery & Arquitetura Final
**Formato:** Game Day em equipe, nos 20 minutos finais da aula
**Entregável:** o relatório gerado por `scripts/lab10.sh report`, com as seções `[preencher]` completas

> Sexta-feira, 18h. A região principal está falhando.
> Recuperem a Tech Pix sem perder nem duplicar dinheiro e provem isso com evidências.

O Lab 23 pedia que vocês **explicassem** um incidente. Aqui vocês vão **operar** um. Os bancos, a replicação, o Kafka e o provider são reais: rodam em containers na sua máquina. Toda decisão tem efeito, e o laboratório mede o resultado.

Não leia o [guia do professor](aula10-professor-guide.md). Ele entrega as armadilhas.

## Preparação (antes da aula, 1 comando)

Pré-requisitos: Docker (Desktop, Colima ou Engine) com `docker compose` ou `docker-compose`, Python 3.9+ e cerca de 3 GB de RAM livres para o Docker.

```bash
scripts/lab10.sh up          # Windows: scripts/lab10.ps1 up
```

A primeira execução faz o build da imagem (alguns minutos). No final, o `status` deve mostrar a réplica em `streaming`, `api ... ready: SIM` e pagamentos sendo liquidados. Rode `scripts/lab10.sh check` e confira os 7 invariantes em `PASS`. **É assim que um sistema saudável se parece.** Guarde essa imagem.

Portas usadas: 8088 (Payment API) e 8090 (provider). O lab não conflita com o stack das aulas 7–9.

## A Tech Pix do laboratório

```text
                    REGIAO PRINCIPAL                                  FORA DA REGIAO
 clientes ──> payment-api ──> pg-primary ══ streaming ══> pg-replica     provider (PSP)
  (loadgen)        │               │ outbox                               liquida o Pix
                   │               v                                       fonte de verdade
                   └─────────> provider <── payment-worker ──> kafka ──> ledger ──> pg-ledger
```

| Componente | O que faz | Detalhe que importa |
|---|---|---|
| `payment-api` | recebe o Pix (com `Idempotency-Key` do cliente) e chama o provider | lê o roteamento **a cada requisição** |
| `payment-worker` | liquida pagamentos consultando o provider e publica o Outbox no Kafka | — |
| `pg-primary` / `pg-replica` | banco do Payment, com réplica assíncrona por streaming | — |
| `ledger` | consome `PaymentCompleted` e lança débito + crédito | consumidor idempotente |
| `provider` | o PSP externo | continua processando durante o desastre |
| `loadgen` | clientes pagando o tempo todo | se não encontram o pagamento que enviaram, **reenviam com a mesma chave** |

## Papéis (equipes de 4–5)

| Papel | Responsabilidade |
|---|---|
| Comandante | declara o incidente, decide, autoriza o failover, controla o relógio |
| Banco | réplica, promoção, fencing, roteamento |
| Reconciliação | provider × banco × Ledger, invariantes |
| Comunicação | o que dizer a clientes e investidores (escrevam a mensagem) |
| Escriba | registra os marcos (`mark`) e monta o relatório |

## O chamado

O instrutor dispara o cenário. Quando aparecer na tela:

```text
18:00 - A REGIAO PRINCIPAL ESTA FALHANDO
trafego em 2x | Payment sem banco primario | Kafka fora | provider processando
```

o relógio começou. A meta de recuperação é a que a turma definiu no slide 9 (o instrutor confirma o valor).

## Missão

**Decidir → recuperar → reconciliar → validar → reabrir.** Nesta ordem.

Sucesso significa:

1. recuperação **dentro da meta**, medida pelo laboratório;
2. **todas** as operações afetadas identificadas, com estado conhecido ou em quarentena;
3. **nenhum** efeito financeiro duplicado;
4. divergências resolvidas ou explicitamente controladas, com responsável.

## Comandos

Todos começam com `scripts/lab10.sh` (Windows: `scripts/lab10.ps1`).

| Comando | Para quê |
|---|---|
| `status` | a visão do plantão: bancos, réplica, roteamento, serviços, filas |
| `logs <servico> [n]` | `api`, `worker`, `ledger`, `loadgen`, `provider`, `primary`, `replica`, `kafka` |
| `mark <evento> [nota]` | registra um marco humano no relógio (use `detectado` quando perceberem a falha) |
| `maintenance on\|off` | suspende/reabre **novos** pagamentos (consultas continuam) |
| `promote --autorizado-por NOME` | promove a réplica a primary. Decisão humana, registrada |
| `fence` | isola o antigo primary da rede: ele perde a autoridade de escrita, mesmo que volte a ligar |
| `route primary\|replica` | troca para onde o `payment-db` aponta |
| `start <servico>` / `restart <servico>` | liga ou reinicia um componente |
| `provider [chave]` | o que o provider sabe |
| `sql <payment\|primary\|replica\|ledger> "SELECT ..."` | consulta direta (`payment` = banco roteado) |
| `reconcile` | compara Tech Pix × provider × Ledger **sem alterar nada** |
| `reconcile --apply` | executa **só** as ações seguras (nenhuma reenvia pagamento ao provider) |
| `check` | verifica os invariantes I1–I7 |
| `report --equipe NOME [--meta-rto MIN]` | gera o relatório com as medições |

## Fases sugeridas

Não há um roteiro passo a passo: o runbook é de vocês. Usem as perguntas para não pular etapas.

### 1. Detectar e declarar (≈3 min)

- O que exatamente está fora? O `health` da API diz o quê? E o `ready`?
- A réplica está em dia? Desde quando não aplica nada?
- Quem é o comandante? Registrem: `mark detectado`, `mark incidente-declarado <nome>`.

### 2. Conter e decidir (≈5 min)

- Os clientes continuam tentando. O que acontece se o sistema voltar a aceitar pagamentos **agora**?
- Promover a réplica: qual é a perda de dados aceita (RPO)? A réplica atende?
- E se o antigo primary voltar depois da promoção?
- Quem autoriza? Registrem a justificativa: ela entra no relatório.

### 3. Recuperar (≈6 min)

- Promover não muda o roteamento. Trocar o roteamento não muda quem **já estava** conectado.
- Kafka fora: o que fica parado enquanto ele não volta?
- Depois de cada passo, `status`. **Todos** os componentes apontam para onde deveriam?

### 4. Reconciliar e validar (≈4 min)

- `reconcile` primeiro, sem `--apply`. Leiam as categorias. Quantas operações foram afetadas? De que tipo?
- Algo ali **não** deve ser corrigido automaticamente?
- `reconcile --apply`, espere alguns segundos e rode `check`.

### 5. Reabrir e relatar (≈2 min)

- Reabram só quando os invariantes permitirem. Depois de reabrir, `check` de novo.
- `report --equipe <nome>`: preencham as seções `[preencher]`.

## Invariantes (`check`)

| | Invariante | O que prova |
|---|---|---|
| I1 | Efeito financeiro único | nenhuma chave do cliente liquidada duas vezes no provider |
| I2 | Ledger consistente | débitos = créditos, 2 lançamentos por pagamento, nenhum órfão |
| I3 | Toda operação conhecida | tudo que o provider liquidou existe na Tech Pix ou está em quarentena |
| I4 | Estados coerentes | nenhum pagamento parado em `PROCESSING`/`UNKNOWN` com o provider já decidido |
| I5 | Ledger completo | todo `COMPLETED` tem lançamentos; nada parado no Outbox |
| I6 | Um único escritor | o antigo primary está isolado (I6b: houve escrita nele depois da promoção?) |
| I7 | Serviço reaberto | API pronta, banco com escrita, manutenção desligada |

A recuperação conta a partir do primeiro `check` com I2–I7 sem `FAIL`. O I1 é avaliado à parte: **um efeito duplicado não se desfaz com reconciliação.**

## Relatório (entregável)

`scripts/lab10.sh report --equipe <nome>` grava `relatorios/game-day-<nome>-<data>.md` com detecção, recuperação, perda observada, transações afetadas, duplicações, divergências, invariantes e a linha do tempo. Vocês completam:

- **Decisão e justificativa:** promoveram ou esperaram? Com qual evidência? Quem autorizou?
- **Divergências e controle:** para cada item em quarentena ou `WARN`, o impacto, a próxima ação e o responsável.
- **O que faríamos diferente:** no runbook e **na arquitetura**.

## Perguntas para depois

1. O `health` respondia `UP` com o banco fora. Que verificação deveria acordar o plantão?
2. Qual foi a perda observada? Ela cabe no RPO que a turma propôs no slide 9?
3. Algum componente continuou apontando para o banco antigo? Como vocês descobriram?
4. Por que a reconciliação **nunca** reenvia um pagamento ao provider?
5. Que mudança de projeto tornaria impossível a duplicação que o laboratório consegue provocar?
6. Que evidência deste relatório convenceria um investidor?

## Entre turmas

```bash
scripts/lab10.sh reset       # apaga tudo e recria do zero (~1 min com a imagem pronta)
scripts/lab10.sh down        # ao final do dia
```

**Próximo problema:** o retorno à região original. Ressincronizar o antigo primary, decidir a janela e validar com o mesmo rigor. É outra operação planejada, não um "desfazer".
