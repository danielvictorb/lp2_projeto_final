# Relatório Técnico — Sistema de Monitoramento IoT

**Aluno:** Daniel Victor Carneiro Brandão da Costa
**Matrícula:** 20230089678


## 1. Arquitetura de comunicação

O sistema usa uma arquitetura de canal duplo, onde cada protocolo de transporte é escolhido de acordo com os requisitos do tipo de tráfego:

O **canal de controle** opera sobre TCP (porta padrão 9000). Ele é utilizado para comandos que exigem entrega confiável e confirmação: iniciar e parar sensores, consultar status e encerrar a sessão. O TCP garante que nenhum comando se perca e que a resposta chegue intacta ao cliente.

O **canal de dados** opera sobre UDP (porta padrão 9001). Ele é utilizado para transmissão contínua de leituras de sensores. Como os dados são gerados a cada 100–500 ms e um valor perdido é rapidamente substituído pelo próximo, a perda eventual de um datagrama é aceitável e não justifica o overhead do TCP.

```
┌─────────────┐           TCP (controle)          ┌─────────────┐
│   Cliente    │◄────────────────────────────────►│  Servidor    │
│              │                                   │              │
│  stdin ──┐   │           UDP (dados)             │  threads     │
│  select()│   │◄──────────────────────────────────│  por sensor  │
│          ▼   │                                   │              │
│  UDP + TCP   │                                   │              │
└─────────────┘                                   └─────────────┘
```


## 2. Estratégia de multiplexação

No **servidor**, a concorrência é implementada com pthreads. A thread principal gerencia o canal TCP usando um loop de recv() com buffer de acumulação. Quando o cliente envia START para um sensor, uma nova thread é criada exclusivamente para aquele sensor. Essa thread gera leituras com variação suave (random walk limitado) e as envia por UDP no intervalo configurado. A flag `active` (volatile int) de cada sensor controla quando a thread deve encerrar.

A escolha de pthreads no servidor se justifica porque cada sensor tem seu próprio timer independente (100 ms, 200 ms ou 500 ms). Usar select() com timeouts calculados para múltiplos timers simultâneos seria mais complexo e frágil do que simplesmente deixar cada thread dormir pelo seu intervalo.

No **cliente**, a multiplexação é feita com select() sobre três descritores: stdin, o socket TCP e o socket UDP. Essa abordagem é ideal para o cliente porque ele é puramente reativo (só precisa responder a eventos de entrada) e não tem timers periódicos próprios. Um único loop sem threads mantém a lógica de estado simples e livre de condições de corrida.


## 3. Fluxo de execução

### 3.1. Servidor

1. Cria o socket TCP com SO_REUSEADDR e faz bind na porta configurada.
2. Cria o socket UDP (sem bind, pois só envia).
3. Entra em accept() aguardando conexão de um cliente.
4. Após aceitar, entra no loop de recebimento TCP com buffer de acumulação.
5. A cada mensagem completa (terminada por `\r\n\r\n`), faz parsing e despacha para o handler correto.
6. START: valida o sensor e o header UdpPort, cria a thread de simulação.
7. STOP: sinaliza a thread via flag `active = 0`.
8. STATUS: monta e envia a resposta com o estado atual.
9. EXIT: para todos os sensores, envia resposta e encerra.

### 3.2. Cliente

1. Conecta ao servidor via TCP.
2. Cria socket UDP e faz bind na porta local configurada.
3. Entra no loop de select().
4. Eventos de stdin: traduz o comando do usuário para o formato do protocolo e envia via TCP. Comandos locais (stats, help) são tratados sem comunicação com o servidor.
5. Eventos no socket TCP: acumula bytes, extrai respostas completas e exibe.
6. Eventos no socket UDP: faz parsing do datagrama, exibe a leitura e atualiza as estatísticas.


## 4. Documentação do protocolo

### 4.1. Protocolo de controle (TCP)

Todas as mensagens seguem o formato texto terminado por `\r\n\r\n`.

**Requisição:**

```
MÉTODO recurso\r\n
Header: valor\r\n
\r\n
```

**Resposta:**

```
CÓDIGO RAZÃO\r\n
Campo1: valor1\r\n
Campo2: valor2\r\n
\r\n
```

**Códigos de status:**

    200 OK — operação realizada com sucesso
    400 Bad Request — comando malformado ou parâmetro ausente
    404 Not Found — sensor não existe
    409 Conflict — operação duplicada (START em sensor já ativo, STOP em sensor já inativo)


### 4.2. Exemplos de mensagens de controle

Iniciar o sensor de temperatura:

```
START /sensor/temperatura\r\n
UdpPort: 9001\r\n
\r\n
```

Resposta:

```
200 OK\r\n
Sensor: temperatura\r\n
Status: streaming\r\n
UdpTarget: 127.0.0.1:9001\r\n
Interval: 100ms\r\n
\r\n
```

Consultar todos os sensores:

```
STATUS /sensors\r\n
\r\n
```

Resposta:

```
200 OK\r\n
Count: 3\r\n
temperatura: streaming (seq=142)\r\n
umidade: inactive\r\n
pressao: inactive\r\n
\r\n
```

Parar um sensor ativo:

```
STOP /sensor/temperatura\r\n
\r\n
```

Resposta:

```
200 OK\r\n
Sensor: temperatura\r\n
Status: stopped\r\n
\r\n
```

Tentar parar um sensor inativo (conflito):

```
STOP /sensor/umidade\r\n
\r\n
```

Resposta:

```
409 Conflict\r\n
Error: sensor 'umidade' ja esta inativo\r\n
\r\n
```

Sensor inexistente:

```
START /sensor/vento\r\n
UdpPort: 9001\r\n
\r\n
```

Resposta:

```
404 Not Found\r\n
Error: sensor 'vento' nao existe\r\n
\r\n
```


### 4.3. Protocolo de dados (UDP)

Cada datagrama contém exatamente uma leitura no formato:

```
SEQ:<n>|SENSOR:<tipo>|VALUE:<valor>|UNIT:<unidade>|TS:<timestamp>
```

Exemplo real capturado durante a execução:

```
SEQ:42|SENSOR:temperatura|VALUE:27.58|UNIT:C|TS:1775619504.798
```

Os campos são:

    SEQ — número de sequência, começa em 1 para cada sensor e incrementa a cada envio
    SENSOR — nome do sensor (temperatura, umidade, pressao)
    VALUE — leitura simulada com duas casas decimais
    UNIT — unidade de medida (C, %, hPa)
    TS — timestamp Unix com milissegundos


## 5. Análise de desempenho

Esta seção apresenta a estrutura para análise de desempenho com os três sensores ativos simultaneamente durante 60 segundos.

### 5.1. Expectativa teórica

Com os três sensores ativos por 60 segundos, o número esperado de datagramas é:

    temperatura: 60s / 0.1s = 600 pacotes
    umidade: 60s / 0.2s = 300 pacotes
    pressao: 60s / 0.5s = 120 pacotes
    Total: 1020 pacotes

### 5.2. Resultados observados

Execução realizada em ambiente localhost (macOS) com os três sensores ativos simultaneamente durante aproximadamente 62 segundos.

| Sensor       | Esperado | Enviados (servidor) | Recebidos (cliente) | Perdidos | Taxa de perda |
|--------------|----------|---------------------|---------------------|----------|---------------|
| temperatura  | ~600     | 626                 | 617                 | 0        | 0.0%          |
| umidade      | ~300     | 320                 | 310                 | 0        | 0.0%          |
| pressao      | ~120     | 130                 | 124                 | 0        | 0.0%          |

A diferença entre "enviados" e "recebidos" se deve ao fato de que os sensores continuaram enviando dados nos breves intervalos entre o comando `stats` e os comandos `stop`. Nenhum pacote foi perdido em nenhum dos sensores.

Saída do comando `stats` capturada durante a execução:

```
=== Estatisticas Locais ===
temperatura  recebidos=617, perdidos=0 (0.0%), min=23.44, max=29.15, media=26.54
umidade      recebidos=310, perdidos=0 (0.0%), min=56.99, max=67.75, media=61.64
pressao      recebidos=124, perdidos=0 (0.0%), min=1006.89, max=1010.93, media=1008.98
```

### 5.3. Análise dos valores simulados

| Sensor       | Mínimo  | Máximo  | Média   | Unidade |
|--------------|---------|---------|---------|---------|
| temperatura  | 23.44   | 29.15   | 26.54   | C       |
| umidade      | 56.99   | 67.75   | 61.64   | %       |
| pressao      | 1006.89 | 1010.93 | 1008.98 | hPa     |

Todos os valores ficaram dentro das faixas configuradas (temperatura: 15–40 °C, umidade: 30–90 %, pressão: 990–1030 hPa). A média de cada sensor ficou próxima do centro da faixa, o que é esperado para um random walk simétrico com clamping nas bordas. A amplitude observada (max − min) é relativamente pequena comparada à faixa total, indicando que o passo do random walk (1% da amplitude da faixa) produz variações suaves e realistas ao longo de 60 segundos.

### 5.4. Observações

Em ambiente localhost, a taxa de perda de pacotes UDP foi zero para todos os sensores, já que não há congestionamento de rede real, latência significativa ou descarte de pacotes por roteadores intermediários. Em uma rede com perda real, o número de sequência permitiria ao cliente quantificar a degradação sem implementar mecanismos de retransmissão.

O sensor de temperatura, com intervalo de 100 ms, gera o maior volume de tráfego (≈10 datagramas/segundo). Mesmo com os três sensores ativos simultaneamente (totalizando ≈17 datagramas/segundo), o sistema não apresentou perdas, indicando que a capacidade do loopback e do buffer UDP é suficiente para essa carga.


## 6. Análise crítica

### 6.1. Por que TCP para o canal de controle?

Comandos como START e STOP precisam de entrega garantida. Se um comando STOP se perdesse, o servidor continuaria enviando dados indefinidamente. O TCP garante entrega ordenada e sem perda, além de fornecer confirmação implícita: se o send() retorna sucesso e a conexão permanece aberta, sabemos que o dado foi entregue ao buffer do receptor. Para mensagens de controle que são esporádicas e de baixo volume, o overhead do TCP (handshake, ACKs, controle de congestionamento) é desprezível.

### 6.2. Por que UDP para o canal de dados?

Leituras de sensores são geradas continuamente a intervalos curtos. Uma leitura perdida é imediatamente substituída pela próxima, tornando a retransmissão desnecessária e até prejudicial (uma leitura retransmitida chega com atraso e já foi superada por dados mais recentes). O UDP elimina o overhead do controle de fluxo e congestionamento do TCP, reduzindo a latência de cada datagrama. Além disso, o UDP permite que o servidor envie dados para o cliente sem manter estado de conexão no nível de transporte.

### 6.3. O que o número de sequência reimplementa do TCP?

O campo SEQ nos datagramas UDP reimplementa parcialmente a funcionalidade de **detecção de perda** do TCP. No TCP, cada segmento carrega um número de sequência que permite ao receptor identificar lacunas e solicitar retransmissão. No nosso protocolo UDP, o SEQ permite ao cliente detectar que pacotes foram perdidos (quando o SEQ pula de N para N+k com k > 1), mas sem a etapa de retransmissão. É uma reimplementação parcial e deliberada: queremos a visibilidade sobre perdas para fins de estatística, mas não a recuperação, que não faz sentido para dados de sensores em tempo real.

### 6.4. Por que não implementar retransmissão?

Retransmitir dados de sensores violaria o propósito do canal UDP. Uma leitura de temperatura feita há 2 segundos e retransmitida agora é menos útil do que a leitura atual. A retransmissão introduziria complexidade significativa (buffers de retenção, timers de timeout, tratamento de duplicatas, reordenação) para um benefício nulo ou negativo. O sistema é projetado sob o princípio de que dados de sensores são efêmeros: um valor perdido é aceitável porque o próximo valor é igualmente informativo.

### 6.5. Limitações de escalabilidade

A implementação atual tem algumas limitações conhecidas:

O servidor aceita um cliente por vez. Para suportar múltiplos clientes simultâneos, seria necessário usar threads ou processos filhos para cada conexão TCP, além de manter estruturas de estado separadas por cliente.

Cada sensor ativo consome uma thread no servidor. Com dezenas de sensores, o número de threads cresce linearmente. Uma alternativa mais escalável seria usar um timer centralizado (epoll + timerfd no Linux) para agendar os envios de todos os sensores em uma única thread.

A transmissão UDP é unicast. Para múltiplos clientes recebendo os mesmos dados, seria mais eficiente usar multicast UDP.

O protocolo de controle é baseado em texto, o que facilita debugging mas tem overhead de parsing comparado a um protocolo binário.

### 6.6. Comparação com HTTP

O protocolo de controle TCP deste projeto tem semelhanças estruturais com HTTP: uma linha de requisição com método e recurso, seguida de headers, seguida de corpo. No entanto, existem diferenças fundamentais:

HTTP opera sobre requisição-resposta estrita, com semântica padronizada para métodos (GET, POST, PUT, DELETE), códigos de status, content negotiation, cache, cookies e muito mais. O nosso protocolo usa apenas quatro métodos próprios (START, STOP, STATUS, EXIT) com semântica específica para o domínio de sensores.

HTTP não foi projetado para manter canais de dados paralelos. Para transmitir leituras em tempo real sobre HTTP, seria necessário usar Server-Sent Events, WebSockets ou polling contínuo, todos significativamente mais complexos do que o UDP direto.

Usar HTTP traria a vantagem de interoperabilidade (qualquer cliente HTTP poderia interagir com o servidor), mas ao custo de overhead desnecessário para um sistema embarcado simples. A abordagem com protocolo próprio é mais leve, didática e adequada ao escopo acadêmico do projeto.
