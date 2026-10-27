---
title: 'O que é Stratum? Como o minerador fala com o pool'
description: 'Acompanhe o Stratum entre seine e bntpool.com: login, trabalhos, alvos de shares, envios e respostas, e entenda cada etapa da conexão do seu minerador.'
tags: ['Fundamentos de mineração', 'Stratum', 'Mineração em pool']
faq:
  - q: 'O que o Stratum faz na mineração em pool?'
    a: 'O Stratum transporta a conversa entre minerador e pool: identifica a sessão, entrega trabalho e leva shares e respostas. O minerador calcula hashes localmente. O pool conecta esse trabalho ao seu nó e mantém a contabilidade necessária ao seu sistema de pagamentos.'
  - q: 'Um endereço Stratum é igual a um endereço de site?'
    a: 'Eles atendem conexões diferentes. O site bntpool.com usa HTTPS para páginas e API; o endpoint de mineração documentado é stratum+tcp://bntpool.com:3333. Digite esse endpoint na configuração do pool do minerador. Abrir o site com sucesso, por si só, não estabelece uma conexão de mineração.'
  - q: 'Uma share Stratum aceita significa pagamento enviado?'
    a: 'Não. A resposta de share informa o resultado de um envio na sessão. Verificação, bloco válido na rede, elegibilidade para recompensas e pagamento concluído são etapas diferentes. Acompanhe a atividade aceita em My Stats e consulte o guia de pagamentos para entender confirmações, PPLNS e o limite mínimo.'
sourceHash: '5cc5086d7b28'
translation: draft
---

Acompanhe um trabalho de bntpool.com até o seine e uma share de volta ao pool. Entender essa conversa facilita ler conexões, atualizações e respostas. Este guia descreve a implementação de bntpool.com; para configurar endpoint e endereço de pagamento, use o [guia do pool](/blog/pt-br/mine-blocknet-on-bntpool/).

## O que é Stratum na mineração?

<Answer>
Stratum é a camada de comunicação usada para receber trabalho do pool e enviar resultados. Com seine e bntpool.com, uma conexão TCP persistente leva mensagens JSON de login, trabalhos e shares. Sua máquina calcula os hashes localmente; o pool obtém modelos de bloco do nó e processa o trabalho enviado.
</Answer>

*O minerador envia login e shares ao pool, recebe trabalhos e respostas, e o pool troca modelos e candidatos a bloco com seu nó.*

## O endpoint identifica a conexão de mineração

O endpoint documentado é `stratum+tcp://bntpool.com:3333`. O esquema indica o transporte, o hostname identifica o serviço e a porta seleciona o listener de mineração. A conexão do navegador com `https://bntpool.com` tem outra finalidade. Copie o endpoint completo de [Get Started](/start) para a configuração do pool para abrir a conexão correta.

A conexão persistente permite mensagens dos dois lados quando há trabalho. bntpool usa JSON delimitado por novas linhas, com IDs que relacionam solicitações e respostas. Esses detalhes descrevem seu formato; o minerador precisa suportá-lo, suas capacidades e o algoritmo Blocknet. O [código do protocolo](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) registra os campos atuais.

## O que acontece no login?

O cliente envia `login` com endereço de pagamento, nome do rig, versão do protocolo e capacidades suportadas. O pool verifica e devolve informações do protocolo e capacidades obrigatórias. Isso define a troca de trabalho na sessão. Uma conexão TCP é o primeiro passo; login aceito mostra que a conversa da aplicação também começou.

O nome do rig identifica uma máquina sob um endereço. Use nomes diferentes para vários rigs, mantendo o endereço de pagamento desejado. Login precisa do endereço receptor, não da frase de recuperação nem da chave privada de gasto. Depois que a sessão é aceita e há trabalho, o pool envia `job` para iniciar o hashing.

## O que contém um trabalho de mineração?

O trabalho inclui IDs da atribuição e do modelo, dados do cabeçalho, altura, alvo de share, alvo da rede e intervalo de nonces. O nonce é o valor modificado durante o hashing. O intervalo define o espaço de busca da sessão. O ID permite relacionar um envio posterior ao trabalho e às condições originalmente atribuídas.

O alvo de share determina quais resultados enviar como evidência de trabalho. O alvo da rede determina possíveis candidatos a bloco. A maioria das shares serve à contabilidade sem virar bloco. O [guia de dificuldade](/blog/pt-br/mining-difficulty-explained/) explica alvo e trabalho esperado; o protocolo entrega esses alvos como parte da atribuição.

## Trabalhos mudam com a conexão aberta

Um trabalho novo não significa, sozinho, que houve desconexão. O pool pode atualizar o trabalho quando muda o modelo do nó ou enviar uma atribuição com nova dificuldade de share. A dificuldade variável adapta o limite de envio ao fluxo observado. O minerador segue a nova atribuição enquanto a conexão continua levando mensagens nos dois sentidos.

Diferencie IDs de trabalho e de modelo nos logs técnicos. Várias atribuições podem usar o mesmo modelo com detalhes de sessão distintos. A altura não descreve toda mudança. Para o operador, verifique chegada de trabalhos, progresso do hashing e respostas aos envios; o [guia de acompanhamento](/blog/pt-br/track-your-miner-on-bntpool/) mostra essa atividade.

## O que contém o envio de uma share?

Ao encontrar um resultado adequado, o seine envia `submit` com trabalho, nonce e hash declarado. O pool processa no contexto da atribuição e responde com o ID da solicitação. O hash declarado integra a mensagem; verificação e validação determinam o próximo estágio. Um candidato a bloco é enviado ao daemon para validação.

A resposta pode informar aceitação, estado de verificação e dificuldade. Leia os campos separadamente, sem interpretar toda resposta bem-sucedida como descoberta de bloco. Contabilidade e pagamentos acontecem além dessa conversa. O [guia de shares](/blog/pt-br/accepted-rejected-stale-shares/) explica os resultados; o [guia PPLNS](/blog/pt-br/how-pplns-payouts-work/) relaciona trabalho elegível, recompensas e pagamentos.

## Como usar essa sequência para verificar o minerador?

Leia na ordem: endpoint alcançado, login aceito, trabalho recebido, hashing ativo e envios respondidos. Se a conexão cair, o cliente reconecta e estabelece a sessão antes de receber mais trabalho. Use a mensagem exata e o horário para localizar a etapa. O site funcionando não prova que a porta de mineração é acessível daquela máquina.

Abra [My Stats](/stats) para o endereço desejado e confira o [status do pool](/status). Após início ou reconexão, dê tempo para o hashrate estimado acumular shares. Separe protocolo de prazo de pagamento: login prepara a sessão, trabalhos fornecem tarefas, shares relatam resultados e a contabilidade determina as etapas seguintes das recompensas.

<CTA target="pool" title="Conecte o seine ao bntpool.com" text="Use a página de configuração e acompanhe trabalhos, shares e seu rig em My Stats." />
