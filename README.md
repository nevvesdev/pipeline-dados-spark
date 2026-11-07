# Pipeline de Dados com Spark e Databricks

Pipeline de dados em camadas (arquitetura medallion) com Spark no Databricks, processando corridas de táxi de Nova York até tabelas Delta prontas para análise.

## Perguntas de negócio

1. Qual o faturamento por hora do dia?
2. Quais CEPs de embarque concentram mais faturamento?
3. Como horário e região se combinam? Onde e quando está o pico?

## Arquitetura

| Camada | Função |
|---|---|
| 🥉 **Bronze** | Ingestão dos dados brutos, sem alterações, em tabela Delta |
| 🥈 **Silver** | Limpeza, padronização e colunas derivadas (duração da corrida, hora do dia) |
| 🥇 **Gold** | Métricas agregadas para responder às perguntas de negócio |

Cada camada vive em um schema próprio no Unity Catalog:

| Schema | Camada |
|---|---|
| `nyctaxi_bronze` | Bronze |
| `nyctaxi_silver` | Silver |
| `nyctaxi_gold` | Gold |

## Estrutura

```
pipeline-dados-spark/
├── README.md
├── .gitignore
└── notebooks/
    ├── 00_setup.py
    ├── 01_bronze_ingestao.py
    ├── 02_silver_limpeza.py
    └── 03_gold_metricas.py
```

## Tecnologias

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)

## Roadmap

- [x] **Fase 0:** repositório, integração Databricks + GitHub, schemas e validação do ambiente
- [x] **Fase 1:** camada bronze (ingestão)
- [x] **Fase 2:** camada silver (limpeza e padronização)
- [ ] **Fase 3:** camada gold (métricas de negócio)
- [ ] **Fase 4:** orquestração, dashboard e conclusões

## Fonte dos dados

Tabela de exemplo `samples.nyctaxi.trips`, disponível no próprio Databricks. Contém corridas de táxi de Nova York de janeiro e fevereiro de 2016, com horário de embarque e desembarque, distância, tarifa e CEP de origem e destino.

## 📝 Registro das decisões

### Fase 0

- **Notebooks em formato source (`.py`) dentro de uma Git folder:** o diff no GitHub fica legível e o histórico por fase fica limpo.
- **Um schema por camada:** separa responsabilidades e facilita permissões e leitura das tabelas.
- **Pasta `notebooks/` na raiz do repositório:** mantém a raiz limpa e deixa espaço para documentação e imagens.
- **Fonte única, a tabela de exemplo do Databricks:** o foco do projeto é a arquitetura em camadas e a análise, sem depender de download ou upload de arquivos. O projeto fica reprodutível por qualquer pessoa com uma conta gratuita.
- **Faturamento = soma de `fare_amount`:** a base não tem gorjeta nem taxas, então faturamento aqui é a tarifa das corridas.
- **Região = CEP de embarque (`pickup_zip`):** a base não traz nome de bairro nem de zona.

### Fase 1

- **Colunas de controle na bronze (`_source_table`, `_ingested_at`):** rastreabilidade da origem sem alterar os dados brutos.
- **Carga com `overwrite`:** a execução é idempotente, e reprocessar não duplica dados.

### Fase 2

- **Limpeza orientada por regras nomeadas:** cada regra é medida isoladamente antes do descarte e registrada na tabela `quality_log`.
- **`dropoff_zip` nulo não descarta a linha:** as perguntas de negócio usam apenas o CEP de embarque.
- **Colunas derivadas na silver:** `pickup_date`, `pickup_hour`, `pickup_dayofweek` e `trip_duration_min`, prontas para as agregações da gold.
- **Log de qualidade com `append`:** cada execução adiciona seus resultados com a data, preservando o histórico.

| Regra | Linhas afetadas |
|---|---|
| `trip_distance_nao_positiva` | 76 |
| `fare_amount_nao_positivo` | 10 |
| `desembarque_nao_posterior_ao_embarque` | 1 |
| `campos_obrigatorios_nulos` | 0 |
| `pickup_zip_nulo` | 0 |

Das 21.932 linhas da bronze, 21.847 chegaram à silver e 85 (0,39%) foram descartadas. Uma mesma linha pode quebrar mais de uma regra, então a soma por regra pode passar do total descartado.