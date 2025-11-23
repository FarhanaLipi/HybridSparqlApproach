import requests
import sys
import json
import os
from collections import defaultdict
from rdflib import Graph, URIRef, Literal, Namespace, BNode
from rdflib.namespace import RDF, RDFS, DC, XSD, VOID as rdflib_VOID, QB
import networkx as nx
import statistics
from datetime import datetime
import psutil
import os
import time
import traceback
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from collections import deque
from typing import Dict, List, DefaultDict
from collections import defaultdict
from typing import Dict, Optional, List, Any
from sqlite import SQLiteIntermediateStore
import gc




class MemoryMonitor:
    def __init__(self, visualizer=None):
        self.process = psutil.Process(os.getpid())
        self.server_memory_gb = 4 
        
        self.last_memory = 0
        self.peak_memory = 0
        
    def get_client_memory(self):
      
        return self.process.memory_info().rss / (1024 ** 3)
        
    def record_memory(self, batches_processed, current_batch_size):
        current_mem = self.get_client_memory()
        self.peak_memory = max(self.peak_memory, current_mem)
        
        overhead = max(0, self.peak_memory - self.last_memory)
        self.last_memory = current_mem
        self.peak_memory = current_mem 
        
        return{
            "client_memory_gb": current_mem,
            "server_memory_gb": self.server_memory_gb,
            "total_memory_gb": current_mem + self.server_memory_gb,
            "actual_overhead_gb": overhead,
            "overhead_per_triple_gb": overhead/current_batch_size if current_batch_size > 0 else 0}
        


        
class BatchReport:
    def __init__(self):
        self.reports = []
        self.start_time = time.time()
        self.report_filename = os.path.join(os.getcwd(), "batch_report.json")
        print(f"Report will be saved to: {self.report_filename}") 

    def add_report(self, batch_number, batch_size, memory_usage_gb, actual_overhead_gb, 
              overhead_per_triple_gb, total_memory_gb, processing_time=None, 
              triples_per_sec=None, spilled_to_disk=None, current_batch_size=None,
              memory_pressure=None, emergency_mode=False):
      
        report = {
            "batch_number": batch_number,
        "batch_size": batch_size,
        "memory_usage_gb": memory_usage_gb,
        "actual_overhead_gb": actual_overhead_gb,
        "overhead_per_triple_gb": overhead_per_triple_gb,
        "total_memory_gb": total_memory_gb,
        "timestamp": datetime.now().isoformat(),
        "processing_time": time.time() - self.start_time,
        "emergency_mode": emergency_mode
        }
        
       
        if processing_time is not None:
            report["processing_time"] = processing_time
        if triples_per_sec is not None:
            report["triples_per_sec"] = triples_per_sec
        if spilled_to_disk is not None:
            report["spilled_to_disk"] = spilled_to_disk
        if current_batch_size is not None:
            report["current_batch_size"] = current_batch_size
        if memory_pressure is not None:
            report["memory_pressure"] = memory_pressure
        
        
        self.reports.append(report)
        
      
        print(f"\nBatch {batch_number} (Size: {batch_size} triples)")
        print(f"Memory Usage: {memory_usage_gb:.2f} GB")
        print(f"Actual Overhead: {actual_overhead_gb:.3f} GB")
        print(f"Per-Triple Overhead: {overhead_per_triple_gb*1e6:.2f} MB per 1k triples")
        print(f"Total Memory (Client+Server): {total_memory_gb:.2f} GB")
        
       
        if processing_time is not None:
            print(f"Processing Time: {processing_time:.2f}s")
        if triples_per_sec is not None:
            print(f"Processing Rate: {triples_per_sec/1000:.1f}k tps")
        if memory_pressure is not None:
            print(f"Memory Pressure: {memory_pressure:.0%}")
        if spilled_to_disk is not None:
            print(f"Spilled to Disk: {'Yes' if spilled_to_disk else 'No'}")
        if current_batch_size is not None:
            print(f"Next Batch Size: {current_batch_size:,}")
        if emergency_mode:
            print(f"Emergency Mode: {'Yes' if emergency_mode else 'No'}")
        
    def save_to_file(self, filename=None):
       
        filename = filename or self.report_filename
        try:
            with open(filename, 'w') as f:
                json.dump(self.reports, f, indent=2)
            print(f"\nSaved {os.path.abspath(filename)}")  
        except Exception as e:
            print(f"Error {str(e)}")


FUSEKI_QUERY_URL = "http://localhost:3030/gspo_datasets/query"
FUSEKI_UPLOAD_URL = "http://localhost:3030/gspo_datasets/data"


VOID = Namespace("http://rdfs.org/ns/void#")
QB = Namespace("http://purl.org/linked-data/cube#")
DS = Namespace("http://purl.org/vocab/dataset/schema#")
MYVOID = Namespace("http://example.org/void-extensions#")
METRIC = Namespace("http://example.org/metrics/")
DCTERMS = Namespace("http://purl.org/dc/terms/")


METADATA_BASE = "http://example.org/metadata/"
DATA_CUBE_BASE = "http://example.org/data_cube/"

QUERIES = {
    "total_triples": "SELECT (COUNT(*) AS ?count) WHERE { GRAPH ?g { ?s ?p ?o } }",
    "unique_subjects": "SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { GRAPH ?g { ?s ?p ?o } }",
    "unique_predicates": "SELECT (COUNT(DISTINCT ?p) AS ?count) WHERE { GRAPH ?g { ?s ?p ?o } }",
    "unique_objects": "SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE { GRAPH ?g { ?s ?p ?o } }",
   
                        
                        
   
  
    "total_literals": """
        SELECT (COUNT(?literal) AS ?totalLiterals)
        WHERE {
            GRAPH ?g { ?s ?p ?literal } .
            FILTER (isLiteral(?literal))  
        }
    """,
    "blank_nodes": """
        SELECT (COUNT( ?blank) AS ?blankNodeCount)
        WHERE {
            { GRAPH ?g { ?blank ?p ?o } FILTER(isBlank(?blank)) }
            UNION
            { GRAPH ?g { ?s ?p ?blank } FILTER(isBlank(?blank)) }
        }
    """,
    "class_hierarchy_depth": """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT (MAX(?depth) AS ?maxClassDepth) WHERE {
            {
                SELECT ?class (COUNT(?mid)-1 AS ?depth) WHERE {
                    GRAPH ?g { 
                        ?class rdfs:subClassOf* ?mid .
                        ?mid rdfs:subClassOf* ?root .
                        FILTER NOT EXISTS { ?root rdfs:subClassOf ?superRoot }
                    }
                }
                GROUP BY ?class
            }
        }
    """,
    "property_hierarchy_depth": """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX qb: <http://purl.org/linked-data/cube#>
        SELECT (MAX(?depth) AS ?maxPropertyDepth) WHERE {
            {
                SELECT ?property (COUNT(?mid)-1 AS ?depth) WHERE {
                    GRAPH ?g { 
                        ?property rdfs:subPropertyOf* ?mid .
                        ?mid rdfs:subPropertyOf* ?root .
                        FILTER NOT EXISTS { ?root rdfs:subPropertyOf ?superRoot }
                    }
                }
                GROUP BY ?property
            }
        }
    """,
     "triples_per_graph": """
        SELECT ?g (COUNT(*) AS ?count) 
        WHERE { GRAPH ?g { ?s ?p ?o } }
        GROUP BY ?g
        ORDER BY DESC(?count)
    """,
    "subjects_per_graph": """
        SELECT ?g (COUNT(DISTINCT ?s) AS ?count) 
        WHERE { GRAPH ?g { ?s ?p ?o } }
        GROUP BY ?g
        ORDER BY DESC(?count)
    """,
    "predicates_per_graph": """
        SELECT ?g (COUNT(DISTINCT ?p) AS ?count) 
        WHERE { GRAPH ?g { ?s ?p ?o } }
        GROUP BY ?g
        ORDER BY DESC(?count)
    """,
    "objects_per_graph": """
        SELECT ?g (COUNT(DISTINCT ?o) AS ?count) 
        WHERE { GRAPH ?g { ?s ?p ?o } }
        GROUP BY ?g
        ORDER BY DESC(?count)
    """,
    "literals_per_graph": """
        SELECT ?g (COUNT(?literal) AS ?count)
        WHERE {
            GRAPH ?g { ?s ?p ?literal } .
            FILTER (isLiteral(?literal))  
        }
        GROUP BY ?g
        ORDER BY DESC(?count)
    """
}

def calculate_centralities(graph: nx.Graph, top_n: int = 10) -> Dict[str, Any]:
    try:
        betweenness_kwargs = {}
        if graph.number_of_nodes() > 100:
            betweenness_kwargs = {'k': min(100, graph.number_of_nodes()), 'seed': 42}
        
        return {
            'closeness_centrality': nx.closeness_centrality(graph),
            'betweenness_centrality': nx.betweenness_centrality(graph, **betweenness_kwargs)
        }
    except Exception as e:
        print(f"Error calculating centralities: {str(e)}")
        return {}

def query_fuseki(sparql_query: str) -> Optional[Dict]:
    try:
        response = requests.post(
            FUSEKI_QUERY_URL,
            data={"query": sparql_query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=600
        )
        if response.status_code == 200:
            return response.json()
        print(f"Query failed: {response.status_code} - {response.text}")
        return None
    except Exception as e:
        print(f"Query error: {str(e)}")
        return None

def batch_query_fuseki(query_template: str, processor, initial_batch_size: int = 100000):
    offset = 0
    batch_size = initial_batch_size
    
    while True:
        query = f"{query_template} LIMIT {batch_size} OFFSET {offset}"
        response = requests.post(
            FUSEKI_QUERY_URL,
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=600
        )
        
        if not response.ok:
            print(f"Batch query failed at OFFSET {offset}: {response.text}")
            break

        data = response.json()
        bindings = data.get("results", {}).get("bindings", [])
        if not bindings:
            break

        batch = []
        for row in bindings:
            triple = {}
            for var in ["s", "p", "o"]:
                if var in row:
                    triple[var] = row[var]
            batch.append(triple)

       
        actual_processed = processor.process_batch(batch)
        offset += actual_processed  
        batch_size = processor.current_batch_size
        print(f"Processed batch up to OFFSET {offset} (Batch size: {len(batch)}, Processed: {actual_processed})")  

class TripleProcessor:
  def __init__(self, max_memory_mb: int = 800, db_path=":memory:", 
             initial_batch_size: int = 100000, min_batch_size: int = 10000, 
             max_batch_size: int = 125000):
    
   
    self.max_memory_mb = max_memory_mb
    
   
    storage_memory_mb = int(max_memory_mb * 0.9)
    self.storage = SQLiteIntermediateStore(
        db_path=db_path,
        max_memory_mb=storage_memory_mb,  
      
    )
    
    
    self.initial_batch_size = initial_batch_size
    self.current_batch_size = initial_batch_size
    self.min_batch_size = min_batch_size
    self.max_batch_size = max_batch_size
    
    
    self.reduce_threshold = 0.75 
    self.increase_threshold = 0.5  
    
   
    self.memory_monitor = MemoryMonitor()
    self.batch_count = 0
    self.batch_reporter = BatchReport()
    
   
    self.memory_history = deque(maxlen=5) 
    self.batch_times = deque(maxlen=10)    
    self.spill_count = 0                   
        

  def process_batch(self, batch_results: List[Dict]):
    self.batch_count += 1
    actual_batch_size = len(batch_results)
    processed_count = 0
    emergency_triggered = False
    
    try:
      
        current_memory = self.memory_monitor.get_client_memory() * 1024 
        memory_pressure = current_memory / self.max_memory_mb
        
       
        if memory_pressure > 0.80:  
            self.current_batch_size = max(
                self.min_batch_size,
                int(self.current_batch_size * 0.7) 
            )
            print(f"High memory pressure ({memory_pressure:.0%}), reducing batch size to {self.current_batch_size}")
        elif memory_pressure < 0.5: 
            self.current_batch_size = min(
                self.max_batch_size,
                int(self.current_batch_size * 1.2)  
            )
            print(f"Low memory pressure ({memory_pressure:.0%}), increasing batch size to {self.current_batch_size}")

       
      

        
        start_time = time.time()
        processed_count = 0
        emergency_triggered = False
        
        for i, row in enumerate(batch_results):
            
            s = row.get('s', {}).get('value') if isinstance(row.get('s'), dict) else row.get('s')
            p = row.get('p', {}).get('value') if isinstance(row.get('p'), dict) else row.get('p')
            o = row.get('o', {}).get('value') if isinstance(row.get('o'), dict) else row.get('o')

          
           # self.storage.update_metric('triples', int_value=1)
            if s: self.storage.update_metric('subjects', set_item=s)
            if p: self.storage.update_metric('predicates', set_item=p)
            if o: self.storage.update_metric('objects', set_item=o)
            
            #if isinstance(row.get('s'), dict) and row['s'].get('type') == 'bnode':
             #   self.storage.update_metric('blank_nodes', set_item=row['s']['value'])
            #if isinstance(row.get('o'), dict) and row['o'].get('type') == 'bnode':
             #   self.storage.update_metric('blank_nodes', set_item=row['o']['value'])

            #if isinstance(o, str) and ('"' in o or "'" in o):
             #   self.storage.update_metric('literals', int_value=1)
            
            if isinstance(s, str) and s.startswith(('http://', 'https://')):
                #self.storage.update_metric('entities', set_item=s)
                self.storage.update_metric('total_out_degree', int_value=1)
               # self.storage.add_entity_degree(s, 1)
                
            if isinstance(o, str) and o.startswith(('http://', 'https://')):
                #self.storage.update_metric('entities', set_item=o)
                self.storage.update_metric('total_in_degree', int_value=1)
               # self.storage.add_entity_degree(o, 1)

            if (isinstance(s, str) and isinstance(o, str) and 
                s.startswith(('http://', 'https://')) and 
                o.startswith(('http://', 'https://'))):
                s_vocab = self._extract_vocabulary(s)
                o_vocab = self._extract_vocabulary(o)

                if s_vocab != "invalid" and o_vocab != "invalid":
            
                   if s_vocab != o_vocab:
                      self.storage.add_vocabulary_relation(s_vocab, o_vocab)
                      self.storage.update_metric('external_links', int_value=1)
                   else:
                      self.storage.update_metric('internal_links', int_value=1)

            if p == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                vocab = self._extract_vocabulary(o)
                self.storage.add_class_count(vocab, o)
                self.storage.update_metric('classes', set_item=o)
            
            if p:
                vocab = self._extract_vocabulary(p)
                self.storage.add_property_count(vocab, p)
            
            if p and o:
                source_vocab = self._extract_vocabulary(p)
                if isinstance(o, str) and o.startswith(('http://', 'https://')):
                    target_vocab = self._extract_vocabulary(o)
                    if source_vocab != target_vocab and source_vocab != "invalid" and target_vocab != "invalid":
                        self.storage.add_vocabulary_relation(source_vocab, target_vocab)
            
            processed_count += 1
            
            
            if i % 1000 == 0:
                current_mem = self.memory_monitor.get_client_memory() * 1024  
                if current_mem > self.max_memory_mb * 0.90:
                    print(f"EMERGENCY: Memory at {current_mem:.1f}MB, processed {processed_count} of {actual_batch_size}")
                    remaining = batch_results[i+1:] if i+1 < len(batch_results) else []
                    if remaining:
                        emergency_processed = self._handle_memory_emergency(remaining)
                        processed_count += emergency_processed
                    emergency_triggered = True
                    break

       
        processing_time = time.time() - start_time
        current_mem_after = self.memory_monitor.get_client_memory()
        memory_used = current_mem_after - (self.memory_monitor.last_memory or 0)
        

        
        mem_report = self.memory_monitor.record_memory(
                self.batch_count,
                processed_count  )
        
        final_memory_mb = self.memory_monitor.get_client_memory() * 1024
        final_pressure = final_memory_mb / self.max_memory_mb 
            
            
        self.batch_reporter.add_report(
            batch_number=self.batch_count,
            batch_size=processed_count,  
            memory_usage_gb=mem_report['client_memory_gb'],
            actual_overhead_gb=mem_report['actual_overhead_gb'],
            overhead_per_triple_gb=mem_report['overhead_per_triple_gb'],
            total_memory_gb=mem_report['total_memory_gb'],
            processing_time=processing_time,
            triples_per_sec=processed_count/processing_time if processing_time > 0 else 0,
            spilled_to_disk=self.storage.db_path != ":memory:",
            current_batch_size=self.current_batch_size,
            memory_pressure=final_pressure,
            emergency_mode=emergency_triggered  
        )

     
        status_message = (
            f"Processed batch {self.batch_count} ({processed_count:,} of {actual_batch_size:,} triples) | "
            f"Memory: {current_mem_after:.2f}GB ({final_pressure:.0%}) | "
            f"Overhead: {memory_used*1024:.1f}MB | "
            f"Time: {processing_time:.2f}s | "
            f"Rate: {processed_count/processing_time/1000:.1f}k tps | "
            f"Next batch: {self.current_batch_size:,}"
            f"{' (DISK)' if self.storage.db_path != ':memory:' else ''}"
            f"{' (EMERGENCY MODE)' if emergency_triggered else ''}"
        )
        print(status_message)
        gc.collect()
        return processed_count
    
        
    
    except MemoryError:
        remaining = batch_results[i:] if 'i' in locals() else batch_results
        if remaining:
            emergency_processed = self._handle_memory_emergency(remaining)
            processed_count += emergency_processed
        self.current_batch_size = max(
            self.min_batch_size,
            int(self.current_batch_size * 0.5)
        )
        print(f"memory error. Processed {processed_count} of {actual_batch_size}")
        gc.collect()
        return processed_count

  def _handle_memory_emergency(self, remaining_triples):
    
    emergency_batch_size = max(1000, int(self.current_batch_size * 0.2))
    print(f"Processing remaining {len(remaining_triples)} triples in emergency mode")
    
    total_processed = 0
    for i in range(0, len(remaining_triples), emergency_batch_size):
        chunk = remaining_triples[i:i+emergency_batch_size]
        total_processed += len(chunk)
        self._process_emergency_chunk(chunk)
        gc.collect()
  
    self.current_batch_size = max(self.min_batch_size, int(self.current_batch_size * 0.5))
    print(f"EMERGENCY: Processed {total_processed} triples, reduced next batch to {self.current_batch_size}")
    return total_processed
  
  def _process_emergency_chunk(self, chunk):
    
    for row in chunk:
        
        s = row.get('s', {}).get('value') if isinstance(row.get('s'), dict) else row.get('s')
        p = row.get('p', {}).get('value') if isinstance(row.get('p'), dict) else row.get('p')
        o = row.get('o', {}).get('value') if isinstance(row.get('o'), dict) else row.get('o')

        
       # self.storage.update_metric('triples', int_value=1)
        if s: self.storage.update_metric('subjects', set_item=s)
        if p: self.storage.update_metric('predicates', set_item=p)
        if o: self.storage.update_metric('objects', set_item=o)
        
        
        #if isinstance(row.get('s'), dict) and row['s'].get('type') == 'bnode':
         #   self.storage.update_metric('blank_nodes', set_item=row['s']['value'])
        #if isinstance(row.get('o'), dict) and row['o'].get('type') == 'bnode':
           # self.storage.update_metric('blank_nodes', set_item=row['o']['value'])

        
        if isinstance(o, str) and ('"' in o or "'" in o):
            self.storage.update_metric('literals', int_value=1)
        
        
        if isinstance(s, str) and s.startswith(('http://', 'https://')):
           # self.storage.update_metric('entities', set_item=s)
            self.storage.update_metric('total_out_degree', int_value=1)
            #self.storage.add_entity_degree(s, 1)
            
        if isinstance(o, str) and o.startswith(('http://', 'https://')):
            #self.storage.update_metric('entities', set_item=o)
            self.storage.update_metric('total_in_degree', int_value=1)
            #self.storage.add_entity_degree(o, 1)

       
        if (isinstance(s, str) and isinstance(o, str) and 
            s.startswith(('http://', 'https://')) and 
            o.startswith(('http://', 'https://'))):
            s_vocab = self._extract_vocabulary(s)
            o_vocab = self._extract_vocabulary(o)

            if s_vocab != "invalid" and o_vocab != "invalid":
            
              if s_vocab != o_vocab:
                 self.storage.add_vocabulary_relation(s_vocab, o_vocab)
                 self.storage.update_metric('external_links', int_value=1)
              else:
                 self.storage.update_metric('internal_links', int_value=1)
      
        if p == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
            vocab = self._extract_vocabulary(o)
            self.storage.add_class_count(vocab, o)
            self.storage.update_metric('classes', set_item=o)
        
        
        if p:
            vocab = self._extract_vocabulary(p)
            self.storage.add_property_count(vocab, p)
        
        
       # if p and o:
         #   source_vocab = self._extract_vocabulary(p)
        #    if isinstance(o, str) and o.startswith(('http://', 'https://')):
         #       target_vocab = self._extract_vocabulary(o)
          #      if source_vocab != target_vocab and source_vocab != "invalid" and target_vocab != "invalid":
          #          self.storage.add_vocabulary_relation(source_vocab, target_vocab)
    
    
    
    gc.collect()
    if hasattr(self.storage, 'conn'):
        self.storage.conn.execute("PRAGMA shrink_memory")

  def _extract_vocabulary(self, uri: str) -> str:
    if not uri or not isinstance(uri, str):
        return "invalid"
    try:
       
        import re
        cleaned_uri = re.sub(r'[\s\[\]]', '', uri)
        
        if "#" in cleaned_uri:
            return cleaned_uri.split("#")[0] + "#"
        if "/" in cleaned_uri:
            parts = cleaned_uri.split("/")
            return "/".join(parts[:3]) + "/" if len(parts) > 3 else cleaned_uri
        return cleaned_uri
    except:
        return "invalid"

  def get_final_counts(self) -> Dict:
        return self.storage.get_final_counts()

  def close(self):
     self.storage.close()
     self.batch_reporter.save_to_file()  
     

def analyze_vocabulary_network(vocabulary_relations: Dict[str, set[str]]) -> Dict[str, Any]:
    G = nx.DiGraph()
    
    for source, targets in vocabulary_relations.items():
        for target in targets:
            G.add_edge(source, target)
    
    if G.number_of_nodes() == 0:
        return {}
    
    graph_metrics = {
        "number_of_nodes": G.number_of_nodes(),
        "number_of_edges": G.number_of_edges(),
        "is_directed": G.is_directed(),
        "is_connected": nx.is_weakly_connected(G),
        "number_strongly_connected_components": nx.number_strongly_connected_components(G),
        "number_weakly_connected_components": nx.number_weakly_connected_components(G)
    }
    
    centralities = calculate_centralities(G)
    
    results = {
        "graph_metrics": graph_metrics,
        "centralities": {
            "closeness": {},
            "betweenness": {}
        }
    }
    
    if centralities:
        for centrality_type in ["closeness", "betweenness"]:
            if centrality_type + "_centrality" in centralities:
                centrality_data = centralities[centrality_type + "_centrality"]
                sorted_items = sorted(centrality_data.items(), key=lambda x: x[1], reverse=True)[:20]
                results["centralities"][centrality_type] = dict(sorted_items)
    
    return results

def process_metrics(visualizer=None) -> Dict[str, Any]:
    metrics = {
        "total_triples": 0,
        "unique_subjects": 0,
        "unique_predicates": 0,
        "unique_objects": 0,
        "total_literals": 0,
        "distinct_classes": 0,
       
        "vcs": [],
        "vps": [],
        "distinct_vocabularies": 0,
    
        
        "blank_nodes": {"blankNodeCount": 0},
        "class_hierarchy_depth": 0,
        "property_hierarchy_depth": 0,
        "vocabulary_network": {},
        "internal_links": 0,
        "external_links": 0,
        "total_out_degree": 0,
        "total_in_degree": 0,
        "per_graph_metrics": {
            "triples": {},
            "subjects": {},
            "predicates": {},
            "objects": {},
            "literals": {}
           
        }
    }
    

    
    for metric in ["triples_per_graph", "subjects_per_graph", "predicates_per_graph", 
                  "objects_per_graph", "literals_per_graph"]:
        try:
            data = query_fuseki(QUERIES[metric])
            if not data or "results" not in data:
                continue

            for result in data["results"]["bindings"]:
                graph_uri = result["g"]["value"]
                count = int(result["count"]["value"])
                
                metric_key = {
                    "triples_per_graph": "triples",
                    "subjects_per_graph": "subjects",
                    "predicates_per_graph": "predicates",
                    "objects_per_graph": "objects",
                    "literals_per_graph": "literals"
                }[metric]
                
                metrics["per_graph_metrics"][metric_key][graph_uri] = count

        except Exception as e:
            print(f"Error processing {metric}: {str(e)}")

   
    for metric in ["total_triples", "unique_subjects", "unique_predicates", 
                  "unique_objects", "total_literals", "blank_nodes", 
                  
                  "class_hierarchy_depth", "property_hierarchy_depth"]:
        try:
            data = query_fuseki(QUERIES[metric])
            if not data or "results" not in data:
                continue

            bindings = data["results"]["bindings"]
            if not bindings:
                continue

            if metric == "blank_nodes":
                result = bindings[0]
                metrics[metric]["blankNodeCount"] = int(result.get("blankNodeCount", {}).get("value", 0))
            
            elif metric in ["total_triples", "unique_subjects", "unique_predicates", 
                          "unique_objects", "total_literals"]:
                result = bindings[0]
                value = result.get("count", result.get("totalLiterals", {})).get("value", 0)
                metrics[metric] = int(value) if value else 0
            elif metric in ["class_hierarchy_depth", "property_hierarchy_depth"]:
                result = bindings[0]
                depth = result.get("maxClassDepth" if metric == "class_hierarchy_depth" else "maxPropertyDepth", {})
                metrics[metric] = int(depth.get("value", 0))

        except Exception as e:
            print(f"Error processing {metric}: {str(e)}")

    
    print("\nbatch processing")
    processor = None

    try:
        processor = TripleProcessor(max_memory_mb=800)
        
        batch_query_fuseki(
            query_template="SELECT ?g ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o } }",
            processor=processor,
            initial_batch_size=100000 
        )
        
        batch_results = processor.get_final_counts()
    
        metrics["vcs"] = batch_results["vcs"]
        metrics["vps"] = batch_results["vps"]
        metrics["distinct_classes"] = len(batch_results["vcs"])
        
        metrics["internal_links"] = batch_results["metrics"]["internal_links"]
        metrics["external_links"] = batch_results["metrics"]["external_links"]
        metrics["total_out_degree"] = batch_results["metrics"]["total_out_degree"]
        metrics["total_in_degree"] = batch_results["metrics"]["total_in_degree"]


        all_vocabularies = set()
        
       
        for entry in batch_results["vcs"]:
            all_vocabularies.add(entry["vocabulary"])
            
       
        for entry in batch_results["vps"]:
            all_vocabularies.add(entry["vocabulary"])
            
       
        for source, targets in batch_results["vocabulary_relations"].items():
            all_vocabularies.add(source)
            all_vocabularies.update(targets)

       
        metrics["distinct_vocabularies"] = len(all_vocabularies)
        
        if batch_results["vocabulary_relations"]:
            print("\nvocab network")
            network_metrics = analyze_vocabulary_network(batch_results["vocabulary_relations"])
            metrics["vocabulary_network"] = network_metrics
        else:
            print("No vocabulary relations found for network analysis")
   
    finally:
        processor.close()  
       

    return metrics

def create_void_metadata(dataset_uri, metrics):
    
    g = Graph()
    g.bind("void", VOID)
    g.bind("myvoid", MYVOID)
    g.bind("qb", QB)
    g.bind("xsd", XSD)
    
    ds = dataset_uri
    g.add((ds, RDF.type, VOID.Dataset))
    
    # Basic metrics
    for prop, value in [
        (VOID.triples, "total_triples"),
        (VOID.distinctSubjects, "unique_subjects"),
        (VOID.distinctPredicates, "unique_predicates"),
        (VOID.distinctObjects, "unique_objects"),
        (VOID.literals, "total_literals")
    ]:
        if value in metrics:
            g.add((ds, prop, Literal(metrics[value], datatype=XSD.integer)))
    
  
    if "blank_nodes" in metrics and "blankNodeCount" in metrics["blank_nodes"]:
        g.add((ds, MYVOID.blankNodeCount, Literal(metrics["blank_nodes"]["blankNodeCount"], datatype=XSD.integer)))
    
  
    if "distinct_vocabularies" in metrics:
        g.add((ds, MYVOID.distinctVocabularies, Literal(metrics["distinct_vocabularies"], datatype=XSD.integer)))
    
   
    if "class_hierarchy_depth" in metrics:
        g.add((ds, MYVOID.classHierarchyDepth, Literal(metrics["class_hierarchy_depth"], datatype=XSD.integer)))
    if "property_hierarchy_depth" in metrics:
        g.add((ds, MYVOID.propertyHierarchyDepth, Literal(metrics["property_hierarchy_depth"], datatype=XSD.integer)))
    
 
    if "internal_links" in metrics:
        g.add((ds, MYVOID.internalLinks, Literal(metrics["internal_links"], datatype=XSD.integer)))
    if "external_links" in metrics:
        g.add((ds, MYVOID.externalLinks, Literal(metrics["external_links"], datatype=XSD.integer)))
    
   
    if "total_out_degree" in metrics:
        g.add((ds, MYVOID.totalOutDegree, Literal(metrics["total_out_degree"], datatype=XSD.integer)))
    if "total_in_degree" in metrics:
        g.add((ds, MYVOID.totalInDegree, Literal(metrics["total_in_degree"], datatype=XSD.integer)))
   
    
    if "vcs" in metrics:
        class_partition = BNode()
        g.add((ds, VOID.classPartition, class_partition))
        g.add((class_partition, RDF.type, VOID.Dataset))
        
        for entry in metrics["vcs"]:
            part = BNode()
            g.add((class_partition, VOID['partition'], part))
            g.add((part, RDF.type, VOID.Dataset))
            g.add((part, VOID['class'], URIRef(entry["class"]))) 
            #g.add((part, VOID.entities, Literal(entry["usageCount"], datatype=XSD.integer)))
            g.add((part, MYVOID.classUsageCount, Literal(entry["usageCount"], datatype=XSD.integer)))
    
  
    if "vps" in metrics:
        property_partition = BNode()
        g.add((ds, VOID.propertyPartition, property_partition))
        g.add((property_partition, RDF.type, VOID.Dataset))
        
        for entry in metrics["vps"]:
            part = BNode()
            g.add((property_partition, VOID['partition'], part))
            g.add((part, RDF.type, VOID.Dataset))
            g.add((part, VOID.property, URIRef(entry["property"])))
            g.add((part, VOID.entities, Literal(entry["usageCount"], datatype=XSD.integer)))

    if "distinct_classes" in metrics:
        g.add((ds, MYVOID.distinctClasses, Literal(metrics["distinct_classes"], datatype=XSD.integer)))
    
  
    
    if "vocabulary_network" in metrics and "graph_metrics" in metrics["vocabulary_network"]:
        network = metrics["vocabulary_network"]
        graph_metrics = network["graph_metrics"]
        
       
        #for prop, value in [
            #(MYVOID.numberOfVocabularyNodes, "number_of_nodes"),
           # (MYVOID.numberOfVocabularyEdges, "number_of_edges")
       # ]:
          #  if value in graph_metrics:
              #  g.add((ds, prop, Literal(graph_metrics[value], datatype=XSD.integer)))
        
       
        if "centralities" in network:
            closeness = network["centralities"].get("closeness", {})
            betweenness = network["centralities"].get("betweenness", {})

           
            top_n = 5 
            
          
            top_closeness = sorted(closeness.items(), key=lambda x: x[1], reverse=True)[:top_n]
            
            
            top_betweenness = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:top_n]
            
           
            all_top_vocabs = set()
            
           
            for vocab, _ in top_closeness:
                all_top_vocabs.add(vocab)
            
           
            for vocab, _ in top_betweenness:
                all_top_vocabs.add(vocab)
            
           
            for vocab in all_top_vocabs:
                closeness_value = closeness.get(vocab, 0.0)
                betweenness_value = betweenness.get(vocab, 0.0)
                
                obs = BNode()
                g.add((ds, MYVOID.vocabularyCentralityObservation, obs))
                g.add((obs, RDF.type, MYVOID.VocabularyCentrality))
                g.add((obs, MYVOID.vocabulary, Literal(vocab)))
                
                g.add((obs, MYVOID.closenessCentrality, 
                       Literal(closeness_value, datatype=XSD.decimal)))
                g.add((obs, MYVOID.betweennessCentrality,
                       Literal(betweenness_value, datatype=XSD.decimal)))
    
    if "per_graph_metrics" in metrics:
        for graph_uri, count in metrics["per_graph_metrics"]["triples"].items():
            graph_node = URIRef(graph_uri)
            g.add((graph_node, RDF.type, VOID.Dataset))
            g.add((graph_node, VOID.triples, Literal(count, datatype=XSD.integer)))
            
            
            for metric_type in ["subjects", "predicates", "objects", "literals", "blank_nodes"]:
                if metric_type in metrics["per_graph_metrics"] and graph_uri in metrics["per_graph_metrics"][metric_type]:
                    prop = {
                        "subjects": VOID.distinctSubjects,
                        "predicates": VOID.distinctPredicates,
                        "objects": VOID.distinctObjects,
                        "literals": VOID.literals
                        
                    }[metric_type]
                    g.add((graph_node, prop, Literal(
                        metrics["per_graph_metrics"][metric_type][graph_uri], 
                        datatype=XSD.integer
                    )))
            
            
            g.add((ds, VOID.subset, graph_node))
    
    return g

def create_data_cube(dataset_uri, metrics):
    
    g = Graph()
    
   
    g.bind("qb", QB)
    g.bind("metric", METRIC)
    g.bind("xsd", XSD)
    g.bind("dcterms", DCTERMS)
    g.bind("rdfs", RDFS)
    
    
    dsd_uri = URIRef(f"{DATA_CUBE_BASE}dsd")
    g.add((dsd_uri, RDF.type, QB.DataStructureDefinition))
    
   
    dimensions = {
        "graph": URIRef(f"{DATA_CUBE_BASE}dimension/graph"),
        "metric_type": URIRef(f"{DATA_CUBE_BASE}dimension/metricType"),
        "vocabulary": URIRef(f"{DATA_CUBE_BASE}dimension/vocabulary"),
        "class": URIRef(f"{DATA_CUBE_BASE}dimension/class"),
        "property": URIRef(f"{DATA_CUBE_BASE}dimension/property")
    }
    
   
    count_measure = URIRef(f"{DATA_CUBE_BASE}measure/count")
    value_measure = URIRef(f"{DATA_CUBE_BASE}measure/value")
    
   
    for dim_uri, label in [
        (dimensions["graph"], "Graph"),
        (dimensions["metric_type"], "Metric Type"),
        (dimensions["vocabulary"], "Vocabulary"),
        (dimensions["class"], "Class"),
        (dimensions["property"], "Property")
    ]:
        g.add((dsd_uri, QB.component, dim_uri))
        g.add((dim_uri, RDF.type, QB.DimensionProperty))
        g.add((dim_uri, RDFS.label, Literal(label)))
    
   
    for measure_uri, label, datatype in [
        (count_measure, "Count", XSD.integer),
        (value_measure, "Value", XSD.decimal)
    ]:
        g.add((dsd_uri, QB.component, measure_uri))
        g.add((measure_uri, RDF.type, QB.MeasureProperty))
        g.add((measure_uri, RDFS.label, Literal(label)))
        g.add((measure_uri, QB.datatype, datatype))
    
    
    metric_types = {
       
        "total_triples": URIRef(f"{DATA_CUBE_BASE}metricType/totalTriples"),
        "distinct_subjects": URIRef(f"{DATA_CUBE_BASE}metricType/distinctSubjects"),
        "distinct_predicates": URIRef(f"{DATA_CUBE_BASE}metricType/distinctPredicates"),
        "distinct_objects": URIRef(f"{DATA_CUBE_BASE}metricType/distinctObjects"),
        "distinct_classes": URIRef(f"{DATA_CUBE_BASE}metricType/distinctClasses"),
        "total_literals": URIRef(f"{DATA_CUBE_BASE}metricType/totalLiterals"),
        "blank_nodes": URIRef(f"{DATA_CUBE_BASE}metricType/blankNodes"),
        "total_out_degree": URIRef(f"{DATA_CUBE_BASE}metricType/totalOutDegree"),
        "total_in_degree": URIRef(f"{DATA_CUBE_BASE}metricType/totalInDegree"),
        "internal_links": URIRef(f"{DATA_CUBE_BASE}metricType/internalLinks"),
        "external_links": URIRef(f"{DATA_CUBE_BASE}metricType/externalLinks"),
        
        
        "graph_triples": URIRef(f"{DATA_CUBE_BASE}metricType/graphTriples"),
        "graph_subjects": URIRef(f"{DATA_CUBE_BASE}metricType/graphSubjects"),
        "graph_predicates": URIRef(f"{DATA_CUBE_BASE}metricType/graphPredicates"),
        "graph_objects": URIRef(f"{DATA_CUBE_BASE}metricType/graphObjects"),
        "graph_literals": URIRef(f"{DATA_CUBE_BASE}metricType/graphLiterals"),
        
        
        "class_usage": URIRef(f"{DATA_CUBE_BASE}metricType/classUsage"),
        "property_usage": URIRef(f"{DATA_CUBE_BASE}metricType/propertyUsage"),
        "class_hierarchy_depth": URIRef(f"{DATA_CUBE_BASE}metricType/classHierarchyDepth"),
        "property_hierarchy_depth": URIRef(f"{DATA_CUBE_BASE}metricType/propertyHierarchyDepth"),
        "distinct_vocabularies": URIRef(f"{DATA_CUBE_BASE}metricType/distinctVocabularies"),
        
        
        "closeness_centrality": URIRef(f"{DATA_CUBE_BASE}metricType/closenessCentrality"),
        "betweenness_centrality": URIRef(f"{DATA_CUBE_BASE}metricType/betweennessCentrality")
    }
    
   
    datasets = {
        "structural": URIRef(f"{DATA_CUBE_BASE}dataset/structural"),
        "per_graph": URIRef(f"{DATA_CUBE_BASE}dataset/perGraph"),
        "usage_hierarchy": URIRef(f"{DATA_CUBE_BASE}dataset/usageHierarchy"),
        "vocabulary_network": URIRef(f"{DATA_CUBE_BASE}dataset/vocabularyNetwork")
    }
    
    for dataset_uri, label in [
        (datasets["structural"], "Dataset-Level Structural Metrics"),
        (datasets["per_graph"], "Per-Graph Analytical Metrics"),
        (datasets["usage_hierarchy"], "Usage Counts and Hierarchy Metrics"),
        (datasets["vocabulary_network"], "Inter-Vocabulary Network Metrics")
    ]:
        g.add((dataset_uri, RDF.type, QB.DataSet))
        g.add((dataset_uri, RDFS.label, Literal(label)))
        g.add((dataset_uri, DCTERMS.created, Literal(datetime.now().isoformat(), datatype=XSD.dateTime)))
        g.add((dataset_uri, QB.structure, dsd_uri))
    
   
    obs_counter = 0
    
    def create_observation_id():
        nonlocal obs_counter
        obs_counter += 1
        return URIRef(f"{DATA_CUBE_BASE}observation/{obs_counter}")
    
    def add_observation(dataset, dimensions_dict, measure_uri, measure_value):
        obs_uri = create_observation_id()
        g.add((obs_uri, RDF.type, QB.Observation))
        g.add((obs_uri, QB.dataSet, dataset))
        
        for dim_uri, value in dimensions_dict.items():
            if value is not None:
                if isinstance(value, str) and value.startswith(('http://', 'https://')):
                    g.add((obs_uri, dim_uri, URIRef(value)))
                else:
                    g.add((obs_uri, dim_uri, Literal(value)))
        
        g.add((obs_uri, measure_uri, Literal(measure_value)))
        return obs_uri
    
    
    structural_metrics = {
        "total_triples": metrics.get("total_triples", 0),
        "distinct_subjects": metrics.get("unique_subjects", 0),
        "distinct_predicates": metrics.get("unique_predicates", 0),
        "distinct_objects": metrics.get("unique_objects", 0),
        "total_literals": metrics.get("total_literals", 0),
        "distinct_classes": metrics.get("distinct_classes", 0),
        "blank_nodes": metrics.get("blank_nodes", {}).get("blankNodeCount", 0),
        "total_out_degree": metrics.get("total_out_degree", 0),
        "total_in_degree": metrics.get("total_in_degree", 0),
        "internal_links": metrics.get("internal_links", 0),
        "external_links": metrics.get("external_links", 0)
    }
    
    for metric_key, value in structural_metrics.items():
        if value:
            add_observation(
                dataset=datasets["structural"],
                dimensions_dict={
                    dimensions["metric_type"]: metric_types[metric_key]
                },
                measure_uri=count_measure,
                measure_value=value
            )
    
    
    if "per_graph_metrics" in metrics:
        graph_metric_mapping = {
            "triples": "graph_triples",
            "subjects": "graph_subjects", 
            "predicates": "graph_predicates",
            "objects": "graph_objects",
            "literals": "graph_literals"
        }
        
        for graph_uri in metrics["per_graph_metrics"]["triples"].keys():
            for metric_type, metric_key in graph_metric_mapping.items():
                if (metric_type in metrics["per_graph_metrics"] and 
                    graph_uri in metrics["per_graph_metrics"][metric_type]):
                    
                    add_observation(
                        dataset=datasets["per_graph"],
                        dimensions_dict={
                            dimensions["graph"]: graph_uri,
                            dimensions["metric_type"]: metric_types[metric_key]
                        },
                        measure_uri=count_measure,
                        measure_value=metrics["per_graph_metrics"][metric_type][graph_uri]
                    )
    
    
    
   
    if "vcs" in metrics:
        for entry in metrics["vcs"]:
            add_observation(
                dataset=datasets["usage_hierarchy"],
                dimensions_dict={
                    dimensions["vocabulary"]: entry["vocabulary"],
                    dimensions["class"]: entry["class"],
                    dimensions["metric_type"]: metric_types["class_usage"]
                },
                measure_uri=count_measure,
                measure_value=entry["usageCount"]
            )
    
     
    if "vps" in metrics:
        for entry in metrics["vps"]:
            add_observation(
                dataset=datasets["usage_hierarchy"],
                dimensions_dict={
                    dimensions["vocabulary"]: entry["vocabulary"],
                    dimensions["property"]: entry["property"],
                    dimensions["metric_type"]: metric_types["property_usage"]
                },
                measure_uri=count_measure,
                measure_value=entry["usageCount"]
            )
    
    
    hierarchy_metrics = {
        "class_hierarchy_depth": metrics.get("class_hierarchy_depth", 0),
        "property_hierarchy_depth": metrics.get("property_hierarchy_depth", 0),
        "distinct_vocabularies": metrics.get("distinct_vocabularies", 0)
    }
    
    for metric_key, value in hierarchy_metrics.items():
        if value:
            add_observation(
                dataset=datasets["usage_hierarchy"],
                dimensions_dict={
                    dimensions["metric_type"]: metric_types[metric_key]
                },
                measure_uri=count_measure,
                measure_value=value
            )
    
   
    
    if "vocabulary_network" in metrics and "centralities" in metrics["vocabulary_network"]:
        centralities = metrics["vocabulary_network"]["centralities"]
        closeness = centralities.get("closeness", {})
        betweenness = centralities.get("betweenness", {})
        
        
        top_n = 5
        
        top_closeness = sorted(closeness.items(), key=lambda x: x[1], reverse=True)[:top_n]
        top_betweenness = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:top_n]
        
        all_top_vocabs = set()
        for vocab, _ in top_closeness:
            all_top_vocabs.add(vocab)
        for vocab, _ in top_betweenness:
            all_top_vocabs.add(vocab)
        
        for vocab in all_top_vocabs:
            closeness_value = closeness.get(vocab, 0.0)
            betweenness_value = betweenness.get(vocab, 0.0)
            
            
            if closeness_value > 0:
                add_observation(
                    dataset=datasets["vocabulary_network"],
                    dimensions_dict={
                        dimensions["vocabulary"]: vocab,
                        dimensions["metric_type"]: metric_types["closeness_centrality"]
                    },
                    measure_uri=value_measure,
                    measure_value=closeness_value
                )
            
            
            if betweenness_value > 0:
                add_observation(
                    dataset=datasets["vocabulary_network"],
                    dimensions_dict={
                        dimensions["vocabulary"]: vocab,
                        dimensions["metric_type"]: metric_types["betweenness_centrality"]
                    },
                    measure_uri=value_measure,
                    measure_value=betweenness_value
                )
    
    print("Data Cube creation completed!")
    return g

def upload_to_fuseki(file_path: str, graph_name: str):
    try:
        with open(file_path, 'rb') as f:
            response = requests.post(
                f"{FUSEKI_UPLOAD_URL}?graph={graph_name}",
                data=f,
                headers={"Content-Type": "text/turtle"},
                timeout=30
            )
        if response.status_code == 201:
            print(f"Uploaded {file_path} to {graph_name}")
        else:
            print(f"Upload failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Upload error: {str(e)}")

def main():
    print("analysing")

    visualizer = None
    try:
        print("\ndata collection")
        

       
        metrics = process_metrics()

        if not metrics:
            print("Failed to get metrics. Exiting.")
            return

    


       
        print("\nMetadata")
        dataset_uri = URIRef("http://example.org/dataset")
        void_graph = create_void_metadata(dataset_uri, metrics)
        void_graph.serialize("void.ttl", format="turtle")
        cube_graph = create_data_cube(dataset_uri, metrics)
        cube_graph.serialize("cube.ttl", format="turtle")

        
        
        upload_to_fuseki("void.ttl", METADATA_BASE + "main")
        upload_to_fuseki("cube.ttl", DATA_CUBE_BASE + "main")

        print("\nAnalysis completed successfully!")

    except Exception as e:
        print(f"\nError during processing: {str(e)}")
        traceback.print_exc()
    finally:
        print("Cleanup completed. Exiting program.")


if __name__ == "__main__":
    main()