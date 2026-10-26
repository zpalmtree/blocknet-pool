---
title: 'Shares aceitos, rejeitados e obsoletos na mineração'
description: 'Entenda os shares aceitos, rejeitados e obsoletos no bntpool.com, interprete mensagens e veja como a dificuldade influencia suas estatísticas de mineração.'
tags: ['Guia de mineração', 'Shares', 'Solução de problemas']
faq:
  - q: 'Um share aceito significa que encontrei um bloco?'
    a: 'Significa que o pool aceitou um resultado conforme suas regras de processamento. Um bloco precisa cumprir um alvo de rede separado e ser aceito pelo daemon. As recompensas seguem descoberta, validação e pagamento. A confirmação de um share não é uma notificação de pagamento.'
  - q: 'Por que o pool rejeita um share obsoleto?'
    a: 'Stale job indica que o envio pertence a uma atribuição que não é mais elegível, por exemplo expirada ou ligada a um template além do período de tolerância. Verifique novos jobs e a retomada dos shares aceitos. Se persistir, confira conexão e versão do minerador.'
  - q: 'Uma dificuldade maior reduz minha recompensa?'
    a: 'A dificuldade muda o trabalho representado por cada share elegível e a frequência esperada dos envios. Compare trabalho ponderado por dificuldade em períodos equivalentes, não apenas quantidades. A recompensa também depende da janela PPLNS, do trabalho elegível, dos blocos encontrados e das regras de validação.'
sourceHash: 5be830208e6c
translation: draft
---

Use o resultado dos shares no log para decidir se deve continuar, conferir a conexão ou investigar rejeições recorrentes. Este guia explica as respostas do bntpool.com e a dificuldade por trás dos contadores. Comece pelo [guia de conexão](/blog/pt-br/mine-blocknet-on-bntpool/) se necessário, ou pelo [guia de acompanhamento](/blog/pt-br/track-your-miner-on-bntpool/#zero-hashrate) se o endereço não mostrar atividade.

## O que é um share aceito?

<Answer>
Um share é um resultado de mineração enviado contra um alvo atribuído pelo pool. A resposta de aceitação confirma sua entrada no processamento e mostra que trabalho está chegando. Descoberta de blocos, verificação dos shares e elegibilidade para pagamento são etapas distintas; aceitação sozinha não é um pagamento.
</Answer>

O minerador calcula muitos hashes entre os envios. A maioria não cumpre o alvo de share e não é enviada. O contador de aceitos conta resultados de envios, não todos os hashes calculados pela CPU ou GPU. O [guia de hashrate](/blog/pt-br/what-is-hashrate/) diferencia contagem local e estimativa do pool pelo trabalho recebido.

## Qual a diferença entre shares e blocos?

O pool atribui jobs e uma dificuldade à conexão. Cumprir o alvo de share demonstra trabalho naquela dificuldade. Um candidato a bloco também precisa cumprir o alvo de rede e passar pelas verificações do daemon. Encontrar um share não implica adicionar um bloco à cadeia.

O bntpool.com registra shares verificados e provisórios durante a validação. Um envio aceito pode ser provisório; a ponderação para pagamentos segue regras de validação e elegibilidade. Use a aceitação como sinal de conexão e [Minhas estatísticas](/stats) para trabalho e recompensas registrados. O [guia de PPLNS](/blog/pt-br/how-pplns-payouts-work/) explica a sequência após um bloco do pool.

## O que significam shares rejeitados e obsoletos?

Um envio rejeitado não passou pela verificação aplicável. O motivo é mais útil que o contador isolado. Stale job é uma categoria de rejeição: a atribuição perdeu a elegibilidade. Leia o texto exato e compare com eventos simultâneos, como reconexão ou novo job.

| Mensagem | Significado e próxima verificação |
|---|---|
| `stale job` | A atribuição está ausente, expirada, substituída além da tolerância ou ligada a um template antigo indisponível. Confira a retomada dos jobs novos e shares aceitos. |
| `duplicate share` | O mesmo template e nonce já foram enviados. Procure repetições; reenviar o mesmo trabalho não adiciona contribuição. |
| `low difficulty share` | O hash não cumpre o alvo atribuído. Se persistir, confira a versão do minerador e o tratamento de jobs e dificuldade. |
| `nonce out of assigned range` | O nonce está fora da faixa atribuída ao job. Confira a versão do minerador e qualquer proxy ou integração própria. |

Share obsoleto e bloco órfão são etapas diferentes: o primeiro trata de trabalho de uma atribuição; o segundo, da posição do bloco na cadeia. Separe o log de shares da [lista de blocos](/blocks). Uma mensagem stale job sozinha não indica que o pool encontrou um bloco órfão.

## Por que a dificuldade dos shares muda?

<Answer>
A dificuldade variável, ou vardiff, ajusta o alvo da conexão à atividade observada. Dificuldade maior geralmente produz menos shares, cada um representando mais trabalho; menor produz mais shares com menos trabalho. Compare trabalho elegível ponderado por dificuldade no mesmo período, em vez de apenas contar shares.
</Answer>

Exemplo aritmético: dez shares elegíveis de dificuldade 100 somam o mesmo que um de dificuldade 1.000: ambos totalizam 1.000. Isso ilustra a ponderação, sem prometer recompensa ou horários exatos. Shares chegam em intervalos variáveis, e a validação determina o trabalho elegível.

A dificuldade da atribuição importa no envio. Não compare um resultado antigo apenas com a dificuldade mais recente na tela. O pool guarda as atribuições para conferir envios. Um ajuste normal não é um erro, e forçar shares mais fáceis não cria trabalho computacional adicional.

## Como interpretar a taxa de rejeição?

Compare aceitos e rejeitados no mesmo período e escopo. O minerador pode contar uma sessão, enquanto o pool cobre outro intervalo ou agrega máquinas. Reiniciar o minerador muda a janela local, mas não apaga o histórico do endereço no pool.

99 envios aceitos e um rejeitado resultam em 1% de rejeição por contagem. Isso não equivale automaticamente à porcentagem de recompensa perdida: dificuldades podem variar e a contabilização tem regras de elegibilidade. Confira motivos, momentos e continuidade do trabalho aceito antes de mudar opções.

## O que fazer quando as rejeições se repetem?

Comece por uma máquina afetada. Anote versão, mensagem exata, início do problema e se ainda chegam jobs novos. Confira [Status](/status) e depois interrupções na conexão da máquina. Atualize pelas [versões oficiais do seine](https://github.com/zpalmtree/seine/releases) e compare após uma mudança por vez.

Se usar proxy ou integração própria, confira se preserva job, faixa de nonce e dificuldade ao encaminhar envios. Se persistir, forneça um trecho curto do log e os detalhes da máquina pelo canal de suporte do projeto. Remova segredos e dados privados; uma frase de recuperação nunca é necessária para diagnosticar shares.

## Como confirmar que a mineração voltou ao normal?

Procure jobs novos seguidos de shares aceitos e abra [Minhas estatísticas](/stats) com o mesmo endereço de recebimento do minerador. Confirme atividade recente da máquina. Espere a estimativa de hashrate acumular trabalho antes de comparar médias. Depois acompanhe recompensas estimadas, saldo e pagamentos como etapas separadas no guia de PPLNS.

<CTA target="pool" title="Conecte-se ao bntpool.com" text="Siga o guia de configuração, confirme shares aceitos e acompanhe sua atividade pelo endereço de recebimento." />
