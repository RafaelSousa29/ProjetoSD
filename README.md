# TP1 - Serviços de Monitorização Urbana para One Health

**Universidade de Trás-os-Montes e Alto Douro (UTAD)**<br>
**Licenciatura em Engenharia Informática | 3º Ano**<br>
**Unidade Curricular:** Sistemas Distribuídos 2025/2026<br>
**Docentes:** Hugo Paredes, Tiago Pinto, Cristiano Pendão  

## 📌 Sobre o Projeto
Este projeto visa a criação de um sistema distribuído que simula uma infraestrutura de monitorização ambiental urbana, enquadrada no paradigma *One Health*. O sistema recolhe dados ambientais continuamente através de sensores distribuídos pela cidade, permitindo associar estas medições à localização e ao tempo, com o objetivo de apoiar a identificação de riscos para a saúde pública e investigação epidemiológica.

## 🏛 Arquitetura do Sistema
O sistema é composto por três tipos de entidades comunicantes:

* **SENSOR**: Dispositivo (cliente) responsável pela recolha de dados ambientais no espaço urbano. 
    * Implementa uma interface de texto simples para simular a recolha e envio de dados.
    * Pode recolher medições como: temperatura, humidade, qualidade do ar, ruído, PM2.5/PM10, luminosidade e streams de vídeo/imagem.
    * Envia medições e mensagens periódicas de *heartbeat* para o gateway.
* **GATEWAY**: Entidade intermédia (sistema *edge*) responsável por receber, validar, agregar e encaminhar os dados dos sensores. 
    * Gere sensores através de ficheiros de configuração CSV (formato: `sensor_id:estado:zona:[tipos_dados]:last_sync`).
    * Monitoriza os *heartbeats* e identifica sensores indisponíveis.
    * Suporta o atendimento de múltiplos sensores em simultâneo.
* **SERVIDOR**: Entidade responsável pelo armazenamento e análise da informação recolhida.
    * Recebe pedidos de múltiplos gateways concorrentemente.
    * Armazena os dados em ficheiros distintos, organizados por tipo de dado ambiental.

## 🛠 Tecnologias e Detalhes de Implementação
* **Linguagem:** C#
* **Comunicação:** Sockets (TCP/UDP a definir no protocolo)
* **Concorrência:** Utilização de *threads* para atendimento concorrente no GATEWAY e SERVIDOR.
* **Sincronização:** Utilização de *mutexes* para garantir o acesso sequencial seguro aos ficheiros.
* **Funcionalidade Extra:** Armazenamento da informação numa Base de Dados Relacional (avaliada e pontuada extra).

## 📅 Fases de Desenvolvimento e Prazos
O desenvolvimento é incremental e está dividido nas seguintes fases:

1. **Desenho do Protocolo** (16 a 20 de março): Definição das mensagens e estados para o diálogo SENSOR/GATEWAY/SERVIDOR, testado via simulação.
2. **Implementação Básica** (23 a 27 de março): Criação simples do Servidor, Gateway e Sensor com comunicações de início, envio de dados do sensor e finalização.
3. **Operação SENSOR** (7 a 10 de abril): Processamento real de dados no Gateway, atualização de ficheiros de estado e encaminhamento final para o Servidor.
4. **Atendimento Concorrente** (13 a 17 de abril): Implementação de *threads* e *mutexes* no Servidor e Gateway para processamento simultâneo e seguro.
