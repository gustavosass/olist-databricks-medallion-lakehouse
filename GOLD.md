# 🏗️ Arquitetura de Dados - Camada Gold (Data Lakehouse)

A camada **Gold** deste projeto foi desenhada seguindo os princípios de **Modelagem Dimensional (Star Schema)**. O objetivo é transformar os dados limpos e padronizados da camada Silver em tabelas de fatos otimizadas para consumo por ferramentas de BI (Power BI, Tableau) e analistas de negócio.

---

## 1. Fato_Vendas (Visão Financeira)
Esta tabela é o motor de análise de performance comercial, permitindo entender a saúde financeira da operação através do tempo.

* **Questões de Negócio:**
    * Qual é a receita total por mês (MRR)?
    * Quais categorias de produtos geram maior faturamento?
    * Qual o ticket médio por pedido realizado?
* **Fontes (Silver):** `orders`, `order_items`, `products`.
* **Principais Métricas:**
    * `faturamento_bruto`: Soma dos preços dos itens.
    * `vms`: Volume Mensal de Vendas (contagem de pedidos únicos).
    * `ticket_medio`: Razão entre faturamento e volume de pedidos.

---

## 2. Fato_Logistica (Visão Operacional)
Focada em eficiência de malha, esta tabela identifica gargalos no processo de entrega e a performance dos vendedores em relação ao frete.

* **Questões de Negócio:**
    * Qual o tempo médio de entrega (Lead Time) por estado (UF)?
    * Qual o percentual de pedidos entregues após o prazo estimado?
    * Quais rotas (Origem do Vendedor x Destino do Cliente) são mais críticas?
* **Fontes (Silver):** `orders`, `customers`, `sellers`, `order_items`.
* **Principais Métricas:**
    * `dias_entrega_real`: Diferença entre data de compra e entrega efetiva.
    * `status_atraso`: Flag booleana (0 ou 1) para pedidos fora do prazo.
    * `custo_frete_medio`: Valor médio pago pelo transporte por região.

---

## 3. Fato_Satisfacao (Visão de Experiência)
Mede a qualidade do catálogo e do serviço prestado através da percepção direta do consumidor final.

* **Questões de Negócio:**
    * Qual a nota média (Rating) por categoria de produto?
    * Qual a volumetria de avaliações críticas (notas 1 e 2)?
    * Existe correlação direta entre atraso logístico e notas baixas?
* **Fontes (Silver):** `order_reviews`, `order_items`, `products`, `orders`.
* **Principais Métricas:**
    * `avg_score`: Média aritmética das notas de avaliação.
    * `taxa_detracao`: Percentual de notas baixas em relação ao total de reviews.
    * `volume_reviews`: Total de feedbacks recebidos por período.

---


gold/
├── dim_customers
├── dim_sellers
├── dim_products
├── dim_dates
├── fct_vendas
├── fct_logistica
└── fct_satisfacao