# Sistema de Monitoramento IoT com Sensores Simulados

**Aluno:** Daniel Victor Carneiro Brandão da Costa

**Matrícula:** 20230089678

**Disciplina:** Linguagem de Programação 2 — Projeto Final


## 1. Sobre o projeto

Este projeto implementa um sistema de monitoramento de sensores IoT usando a linguagem C.
A comunicação entre servidor e cliente usa dois canais distintos: um canal de controle TCP
para envio de comandos e recebimento de respostas, e um canal de dados UDP para transmissão
contínua de leituras simuladas de sensores.

O servidor simula três tipos de sensor (temperatura, umidade e pressão atmosférica),
gerando leituras com variação suave ao longo do tempo. O cliente exibe essas leituras
em tempo real e acumula estatísticas sobre os dados recebidos.


## 2. Compilação

O projeto requer GCC e foi testado em Ubuntu 22.04. Para compilar ambos os programas:

```
make all
```

Também é possível compilar cada programa separadamente:

```
make server
make client
```

Para limpar os binários gerados:

```
make clean
```


## 3. Execução

Abra dois terminais. No primeiro, inicie o servidor:

```
./sensor_server [porta_tcp]
```

No segundo, inicie o cliente:

```
./sensor_client [host] [porta_tcp] [porta_udp]
```

Todos os parâmetros são opcionais. Os valores padrão são:

    Porta TCP: 9000
    Porta UDP: 9001
    Host: 127.0.0.1

Por exemplo, para conectar ao servidor na mesma máquina com as portas padrão, basta:

```
./sensor_server
./sensor_client
```


## 4. Comandos do cliente

Ao iniciar o cliente, um prompt interativo fica disponível. Os comandos aceitos são:

    start <sensor> [porta_udp]

Inicia o streaming de um sensor. Os sensores disponíveis são `temperatura`, `umidade`
e `pressao`. A porta UDP é opcional e usa o valor padrão se não informada.
Também aceita o formato do protocolo: `START /sensor/temperatura`.

    stop <sensor>

Para o streaming de um sensor ativo.

    status

Consulta o estado de todos os sensores no servidor.

    status <sensor>

Consulta o estado de um sensor específico.

    stats

Comando local que exibe as estatísticas acumuladas de cada sensor que já enviou dados:
pacotes recebidos, pacotes perdidos (com percentual), valor mínimo, máximo e médio.

    quit

Envia o comando EXIT ao servidor e encerra o cliente.

    help

Exibe a lista de comandos disponíveis.


## 5. Exemplo de sessão

```
$ ./sensor_server
[14:30:01] Servidor TCP escutando na porta 9000...
[14:30:01] Aguardando conexao de cliente...
```

Em outro terminal:

```
$ ./sensor_client
Conectado ao servidor 127.0.0.1:9000
Socket UDP escutando na porta 9001
Digite comandos (START, STOP, STATUS, stats, quit):

start temperatura
Resposta: 200 OK | Sensor: temperatura | Status: streaming | UdpTarget: 127.0.0.1:9001 | Interval: 100ms
[UDP] SEQ:1    | temperatura  | 27.58 C   | TS:1775619504.798
[UDP] SEQ:2    | temperatura  | 27.81 C   | TS:1775619504.902
[UDP] SEQ:3    | temperatura  | 27.86 C   | TS:1775619505.003

start umidade
Resposta: 200 OK | Sensor: umidade | Status: streaming | UdpTarget: 127.0.0.1:9001 | Interval: 200ms
[UDP] SEQ:1    | umidade      | 60.29 %   | TS:1775619505.449
[UDP] SEQ:8    | temperatura  | 27.76 C   | TS:1775619505.521

stats
=== Estatisticas Locais ===
temperatura  recebidos=8, perdidos=0 (0.0%), min=27.58, max=27.97, media=27.78
umidade      recebidos=1, perdidos=0 (0.0%), min=60.29, max=60.29, media=60.29

status
Resposta: 200 OK | Count: 3
  temperatura: streaming (seq=42)
  umidade: streaming (seq=12)
  pressao: inactive

stop temperatura
Resposta: 200 OK | Sensor: temperatura | Status: stopped
stop umidade
Resposta: 200 OK | Sensor: umidade | Status: stopped
quit
Enviando EXIT ao servidor...
Conexao encerrada.
```


## 6. Arquitetura

O sistema é composto por dois programas e um header compartilhado:

`protocol.h` contém as definições de constantes, portas padrão, catálogo de sensores
(nome, unidade, faixa de valores, intervalo de envio) e códigos de status do protocolo.

`sensor_server.c` usa o modelo de concorrência baseado em pthreads. A thread principal
gerencia o canal TCP em um loop de recebimento com buffer para lidar com leituras parciais.
Para cada sensor ativado via START, uma nova thread é criada para gerar leituras e enviá-las
por UDP no intervalo configurado.

`sensor_client.c` usa select() para multiplexar três fontes de eventos em um único loop:
a entrada padrão (stdin), o socket TCP e o socket UDP. Isso evita a necessidade de threads
no lado do cliente.

O protocolo de controle TCP usa mensagens de texto terminadas por `\r\n\r\n`, com uma linha
de requisição seguida de headers (pares chave-valor). As respostas seguem o mesmo padrão,
com código de status, headers descritivos e corpo opcional. O protocolo de dados UDP usa
datagramas de texto com campos separados por `|`.
