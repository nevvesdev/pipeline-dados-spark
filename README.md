# 🥉🥈🥇 Pipeline de Dados com Spark e Databricks

Pipeline de dados em camadas (arquitetura medallion) com Spark no Databricks, processando corridas de táxi de Nova York até tabelas Delta prontas para análise.

## 🎯 Perguntas de negócio

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
| `main.nyctaxi_bronze` | Bronze |
| `main.nyctaxi_silver` | Silver |
| `main.nyctaxi_gold` | Gold |

## Estrutura

```
pipeline-dados-spark/
├── README.md
├── .gitignore
├── docs/
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
- [ ] **Fase 2:** camada silver (limpeza e padronização)
- [ ] **Fase 3:** camada gold (métricas de negócio)
- [ ] **Fase 4:** orquestração, dashboard e conclusões

## Fonte dos dados

Tabela de exemplo `samples.nyctaxi.trips`, disponível no próprio Databricks. Contém corridas de táxi de Nova York de janeiro e fevereiro de 2016, com horário de embarque e desembarque, distância, tarifa e CEP de origem e destino.