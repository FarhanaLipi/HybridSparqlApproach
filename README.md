 Hybrid SPARQL-Based RDF Dataset Analysis Framework

A memory-efficient approach for analyzing large RDF datasets on commodity hardware using hybrid SPARQL querying and adaptive memory management.

 Overview

This framework enables comprehensive structural analysis of RDF datasets containing millions of triples while operating under strict memory constraints (500MB-1024MB). It combines server-side SPARQL querying with client-side batch processing and implements an adaptive memory management system that automatically spills intermediate results to disk when memory limits are approached.

Key Features

- Hybrid Architecture: Distributes workload between Apache Jena Fuseki server and Python client
- Adaptive Memory Management: Monitors memory usage and spills to SQLite when thresholds are exceeded
- Batch Processing: Dynamically adjusts batch sizes based on real-time memory pressure
- Comprehensive Metrics: Extracts structural, vocabulary usage, and network analysis metrics
- Standards Compliance: Generates VoID and RDF Data Cube metadata for interoperability
- Resource Efficiency: Successfully processes 7.8M triples with as little as 500MB RAM

Hardware
- Minimum: 4GB RAM, 500MB available for client processing
- Recommended: 8GB+ RAM for better performance
- SSD storage for efficient disk spilling

Software
- Python 3.10+
- Apache Jena Fuseki 5.3.0+
- Java 17+

Python Dependencies
```bash
pip install requests rdflib networkx psutil seaborn matplotlib
