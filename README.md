# ProjetoSD - OneHealth

Sistema distribuido para monitorizacao ambiental urbana no contexto One Health.

O projeto esta organizado em varios processos independentes que comunicam entre si por sockets TCP, RPC e Pub/Sub.

## Componentes

- `Sensor`: simula sensores ambientais, envia medicoes, heartbeats e streams de video.
- `Gateway`: valida sensores, recebe dados via Pub/Sub, faz pre-processamento por RPC, agrega lotes e envia para o servidor.
- `Servidor`: guarda medicoes, videos e analises; disponibiliza dashboard web.
- `RabbitMQ`: broker Pub/Sub usado para entregar medicoes dos sensores ao gateway.
- `PubSubBroker`: broker local antigo/fallback; ja nao e necessario no fluxo principal com RabbitMQ.
- `PreProcessamentoRPC`: servico RPC em Python chamado pelo gateway antes de guardar/agregar medicoes.
- `AnaliseRPC`: servico RPC em Python chamado pelo servidor para calcular estatisticas/risco.
- `DataGenerator`: biblioteca usada pelo sensor para gerar valores simulados.

## Portas

- `5000`, `5001`, `5002`, ...: Gateways podem receber ligacoes de controlo dos sensores.
- `6000`: Servidor recebe lotes enviados pelo gateway.
- `7000`: RPC de pre-processamento.
- `7001`: RPC de analise.
- `5672`: RabbitMQ AMQP.
- `15672`: painel web do RabbitMQ, se o plugin de gestao estiver ativo.
- `8080`: Dashboard web do servidor.

## Como executar

Antes de iniciar os projetos, confirmar que o servico RabbitMQ esta ativo. Se o painel de gestao estiver ligado, podes confirmar em:

```text
http://localhost:15672
```

Os servicos RPC agora correm em Python. Se o comando `python` nao for reconhecido, instalar Python 3 e ativar a opcao `Add python.exe to PATH`.

Abrir um terminal para cada componente, por esta ordem:

```powershell
cd C:\Users\lucas\source\repos\ProjetoSD\PreProcessamentoRPC
python .\preprocessamento_rpc.py
```

```powershell
cd C:\Users\lucas\source\repos\ProjetoSD\AnaliseRPC
python .\analise_rpc.py
```

```powershell
cd C:\Users\lucas\source\repos\ProjetoSD\Servidor
dotnet run
```

```powershell
cd C:\Users\lucas\source\repos\ProjetoSD\Gateway
dotnet run
```

Ao iniciar o Gateway, escolher o ID, a porta e os topicos RabbitMQ. Exemplos:

```text
GW01 / porta 5000 / TEMP
GW02 / porta 5001 / RUIDO
GW03 / porta 5002 / S105
GW04 / porta 5003 / S105:TEMP
```

Nota: a porta de cada gateway serve para ligacoes de controlo (`CONNECT`, `HEARTBEAT`, `NOTIFY`, `VIDEO`). As medicoes chegam por RabbitMQ; cada gateway escolhe os topicos que quer receber.

O formato dos topicos publicados e `DATA.SENSOR_ID.ZONA.TIPO`. Exemplos:

```text
TEMP       -> recebe TEMP de todos os sensores
RUIDO      -> recebe RUIDO de todos os sensores
S105       -> recebe todos os tipos do sensor S105
S105:TEMP  -> recebe apenas TEMP do sensor S105
all        -> recebe tudo
```

Tambem podes usar varios topicos no mesmo gateway:

```text
TEMP,RUIDO
S105,S108:TEMP
DATA.S105.*.TEMP,DATA.S108.*.RUIDO
all
```

```powershell
cd C:\Users\lucas\source\repos\ProjetoSD\Sensor
dotnet run
```

Ao iniciar o Sensor, escolher a ligacao de controlo:

```text
1 - Gateway principal 127.0.0.1:5000
2 - Um Gateway especifico
3 - Varios Gateways
```

Exemplo para ligar o mesmo sensor a `GW01` e `GW02`:

```text
Opcao: 3
Gateways: 5000,5001
```

As medicoes continuam a ser publicadas no RabbitMQ e cada Gateway recebe apenas os topicos que subscreveu.

Depois de iniciar o servidor, abrir:

```text
http://localhost:8080
```

## Funcionalidades principais

- Validacao de sensores no gateway por ficheiro CSV.
- Envio de medicoes por RabbitMQ com exchange topic `onehealth.medicoes`.
- Queues duraveis por gateway/topico, por exemplo `onehealth.gateway.GW01.DATA___TEMP`.
- Pre-processamento RPC antes da agregacao no gateway.
- Agregacao de medicoes no gateway e envio por lotes para o servidor.
- Envio Gateway -> Servidor em lote JSON estruturado com metadata, medicoes e resumo agregado por tipo.
- Persistencia em ficheiros por tipo de dado e em SQLite.
- Analise RPC automatica apos rececao de lotes no servidor.
- Dashboard web com medicoes, estatisticas, filtros, videos recebidos e pedido manual de analise.
- Heartbeats, estados dos sensores, bateria, carregamento e manutencao.
- Streams de video simuladas iniciadas pelo sensor e encaminhadas ate ao servidor.

## Comandos uteis

No Gateway:

- `send`: forca o envio do lote atual para o servidor.
- `status`: lista sensores.
- `create`: cria novo sensor.
- `state`: altera estado de um sensor.

No Servidor:

- `analises`: lista as ultimas analises guardadas.
- `medicoes`: lista as ultimas medicoes recebidas.
- `reanalyze`: pede nova analise RPC usando medicoes ja guardadas.
- `help`: mostra comandos.

## Problemas comuns

Se aparecer erro de porta ocupada ou ficheiro bloqueado, existe uma instancia antiga em execucao:

```powershell
Get-Process Sensor,Gateway,Servidor,python -ErrorAction SilentlyContinue
```

Para parar um processo:

```powershell
Stop-Process -Name Servidor -Force
```

Trocar `Servidor` pelo nome do componente necessario.
