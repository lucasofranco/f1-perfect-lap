# F1 Data Engineering: Perfect Lap Analysis

Este projeto é um pipeline de Engenharia de Dados (ETL) que extrai telemetria de corridas de Fórmula 1, processa os dados para calcular a **"Volta Perfeita Teórica"** (soma dos melhores setores de cada piloto) e compara com a performance real na classificação.

O objetivo é automatizar a análise de lacunas de performance (Gaps) e armazenar os dados processados em um Data Lake na AWS S3 para posterior visualização em Power BI.

## Arquitetura do Projeto

O fluxo de dados segue um pipeline **ETL** (Extract, Transform, Load):

1.  **Ingestão (Extract):** Conexão com a API pública da Fórmula 1 usando a biblioteca `FastF1`.
2.  **Processamento (Transform):**
    * Limpeza de voltas inválidas (in/out laps).
    * Agregação de dados para encontrar os melhores setores (1, 2 e 3) por piloto.
    * Cálculo da "Volta Perfeita" e do "Gap" (diferença) para a volta real.
3.  **Armazenamento (Load):**
    * Salvamento local em CSV para validação.

**Tecnologias Utilizadas:**
**Python 3.12**
**Pandas** (Manipulação e Agregação de Dados)
**FastF1** (Extração de Telemetria)

---

## 📂 Estrutura do Repositório

```bash
f1-data-engineering/
├── data/
│   ├── raw/             # Cache da API (Ignorado no Git)
│   └── processed/       # CSVs finais gerados pelo pipeline
├── notebooks/           # Jupyter Notebooks para análise exploratória
├── src/
│   └── main.py          # Script principal de automação (Loop da Temporada)
├── .env.example         # Exemplo das variáveis de ambiente necessárias
├── .gitignore           # Arquivos ignorados pelo Git
└── requirements.txt     # Dependências do projeto

Projeto desenvolvido por Lucas Franco. www.linkedin.com/in/lucasofranco | lucassoliveiraa56@gmail.com