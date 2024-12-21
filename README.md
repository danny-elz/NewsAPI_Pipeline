# News Analytics Pipeline

This repository implements a pipeline for fetching, streaming, processing, and aggregating news articles. It uses **NewsAPI**, **Apache Kafka**, and **Apache Spark** to process real-time news data and store aggregated insights in HDFS.

---

## Features
- Fetch articles by keywords using the **NewsAPI**.
- Stream news articles to **Kafka** using a Python producer.
- Process and aggregate streamed data using **Apache Spark**.
- Store aggregated results in **HDFS** in Parquet format.
- Read and display aggregated results for further analysis.
