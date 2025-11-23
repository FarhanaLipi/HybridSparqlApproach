import sqlite3
import tempfile
import os
import psutil
from typing import Dict, List, Set, Any, Tuple

class SQLiteIntermediateStore:
    def __init__(self, db_path=":memory:", max_memory_mb=800, spill_threshold=0.85):
       
        self.max_memory_mb = max_memory_mb
        self.spill_threshold = spill_threshold
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._create_tables()
        self.process = psutil.Process(os.getpid())
        
    def _create_tables(self):
       
        cursor = self.conn.cursor()
        
      
        cursor.execute("PRAGMA temp_store = MEMORY;")
        cursor.execute("PRAGMA journal_mode = MEMORY;")
        cursor.execute("PRAGMA synchronous = OFF;")
        cursor.execute("PRAGMA cache_size = -10000;")  
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS class_counts (
            vocab TEXT,
            class_uri TEXT,
            count INTEGER,
            PRIMARY KEY (vocab, class_uri)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS property_counts (
            vocab TEXT,
            property_uri TEXT,
            count INTEGER,
            PRIMARY KEY (vocab, property_uri)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary_relations (
            source_vocab TEXT,
            target_vocab TEXT,
            PRIMARY KEY (source_vocab, target_vocab)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS entity_degrees (
            entity_uri TEXT PRIMARY KEY,
            degree INTEGER
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            metric_name TEXT PRIMARY KEY,
            int_value INTEGER,
            set_count INTEGER
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics_sets (
            metric_name TEXT,
            item TEXT,
            PRIMARY KEY (metric_name, item)
        )
        """)
        
        self.conn.commit()
    
    def _check_memory(self) -> bool:
        
        current_mem = self.process.memory_info().rss / (1024 ** 2)  # MB
        
        
        memory_pressure = current_mem / self.max_memory_mb
        
        if memory_pressure > self.spill_threshold and self.db_path == ":memory:":
            self._spill_to_disk()
            return True
        return False
    
    def _spill_to_disk(self):
       
        try:
           
            temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
            temp_path = temp_db.name
            temp_db.close() 
            
            print(f"Memory threshold exceeded, spilling to disk ")
            
           
            disk_conn = sqlite3.connect(temp_path)
            
           
            self.conn.backup(disk_conn)
            
            
            self.conn.close()
            self.conn = disk_conn
            self.db_path = temp_path
            
           
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.execute("PRAGMA synchronous = NORMAL;")
            self.conn.commit()
            
        except Exception as e:
            print(f"Error during spilling to disk: {str(e)}")
            raise
    
    def add_class_count(self, vocab: str, class_uri: str, count: int = 1):
        self._check_memory()
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO class_counts (vocab, class_uri, count)
        VALUES (?, ?, ?)
        ON CONFLICT(vocab, class_uri) DO UPDATE SET count = count + ?
        """, (vocab, class_uri, count, count))
        self.conn.commit()
    
    def add_property_count(self, vocab: str, property_uri: str, count: int = 1):
        self._check_memory()
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO property_counts (vocab, property_uri, count)
        VALUES (?, ?, ?)
        ON CONFLICT(vocab, property_uri) DO UPDATE SET count = count + ?
        """, (vocab, property_uri, count, count))
        self.conn.commit()
    
    def add_vocabulary_relation(self, source_vocab: str, target_vocab: str):
        self._check_memory()
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT OR IGNORE INTO vocabulary_relations (source_vocab, target_vocab)
        VALUES (?, ?)
        """, (source_vocab, target_vocab))
        self.conn.commit()
    
    def add_entity_degree(self, entity_uri: str, degree: int = 1):
        self._check_memory()
        cursor = self.conn.cursor()
        cursor.execute("""
        INSERT INTO entity_degrees (entity_uri, degree)
        VALUES (?, ?)
        ON CONFLICT(entity_uri) DO UPDATE SET degree = degree + ?
        """, (entity_uri, degree, degree))
        self.conn.commit()
    
    def update_metric(self, metric_name: str, int_value: int = None, set_item: str = None):
        self._check_memory()
        cursor = self.conn.cursor()
        
        if int_value is not None:
            cursor.execute("""
            INSERT INTO metrics (metric_name, int_value)
            VALUES (?, ?)
            ON CONFLICT(metric_name) DO UPDATE SET int_value = int_value + ?
            """, (metric_name, int_value, int_value))
        
        if set_item is not None:
            cursor.execute("""
            INSERT OR IGNORE INTO metrics_sets (metric_name, item)
            VALUES (?, ?)
            """, (metric_name, set_item))
            
            cursor.execute("""
            UPDATE metrics SET set_count = (
                SELECT COUNT(*) FROM metrics_sets WHERE metric_name = ?
            ) WHERE metric_name = ?
            """, (metric_name, metric_name))
        
        self.conn.commit()
    
    def get_final_counts(self) -> Dict[str, Any]:
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT vocab, class_uri, count FROM class_counts ORDER BY count DESC")
        vcs = [
            {"vocabulary": row[0], "class": row[1], "usageCount": row[2]}
            for row in cursor.fetchall()
        ]
        
        cursor.execute("SELECT vocab, property_uri, count FROM property_counts ORDER BY count DESC")
        vps = [
            {"vocabulary": row[0], "property": row[1], "usageCount": row[2]}
            for row in cursor.fetchall()
        ]
        
        cursor.execute("SELECT source_vocab, target_vocab FROM vocabulary_relations")
        vocab_relations = {}
        for row in cursor.fetchall():
            if row[0] not in vocab_relations:
                vocab_relations[row[0]] = set()
            vocab_relations[row[0]].add(row[1])
        
        cursor.execute("SELECT metric_name, int_value, set_count FROM metrics")
        metrics = {}
        for row in cursor.fetchall():
            metrics[row[0]] = row[1] if row[1] is not None else row[2]
        
        cursor.execute("SELECT SUM(degree) FROM entity_degrees")
        total_degree = cursor.fetchone()[0] or 0
        
        return {
            "vcs": vcs,
            "vps": vps,
            "vocabulary_relations": {k: list(v) for k, v in vocab_relations.items()},
            "metrics": {
                'triples': metrics.get('triples', 0),
                'distinctSubjects': metrics.get('subjects', 0),
                'distinctPredicates': metrics.get('predicates', 0),
                'distinctObjects': metrics.get('objects', 0),
                'distinctClasses': metrics.get('classes', 0),
                'literals': metrics.get('literals', 0),
                'blankNodes': metrics.get('blank_nodes', 0),
                'distinctEntities': metrics.get('entities', 0),
                'internal_links': metrics.get('internal_links', 0),
                'external_links': metrics.get('external_links', 0),
                'total_out_degree': metrics.get('total_out_degree', 0),
                'total_in_degree': metrics.get('total_in_degree', 0)
            },
            "degree_stats": {
                "total_out_degree": metrics.get('total_out_degree', 0),
                "total_in_degree": metrics.get('total_in_degree', 0)
            }
        }
    
    def close(self):
        """Close the database connection and clean up"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
        if hasattr(self, 'db_path') and self.db_path != ":memory:" and os.path.exists(self.db_path):
            try:
                os.unlink(self.db_path)
            except:
                pass
    
    def __del__(self):
        self.close()