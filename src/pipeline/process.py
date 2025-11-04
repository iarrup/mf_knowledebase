import sys
import logging
import psycopg2
import json
from src.core.logger import setup_logging

from src.analysis.summarizer import get_summary
# NEW: Import shared functions
from src.database.vector_utils import get_db_connection, vectorize_and_store

logger = logging.getLogger(__name__)

# --- Main Processing Logic ---
def process_unsummarized_data(conn):
    """Finds unprocessed code, generates summaries, and creates code vectors."""
    logger.info("\n--- Processing Code Summaries and Vectors ---")
    try:
        # --- 1. Process Divisions ---
        with conn.cursor() as cursor:
            cursor.execute("SELECT d.division_id, d.code, d.program_id FROM cobol_divisions d WHERE d.summary IS NULL;")
            items = cursor.fetchall()
            logger.info(f"Found {len(items)} divisions to process.")
            for item_id, code, prog_id in items:
                logger.info(f"Processing division_id: {item_id}...")
                summary = get_summary(code)
                cursor.execute("UPDATE cobol_divisions SET summary = %s WHERE division_id = %s;", (summary, item_id))
                vectorize_and_store(cursor, code, summary, vector_type='code', 
                                    program_id=prog_id, division_id=item_id)
        conn.commit()

        # --- 2. Process Sections ---
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT s.section_id, s.code, d.program_id, s.division_id
                FROM cobol_sections s JOIN cobol_divisions d ON s.division_id = d.division_id
                WHERE s.summary IS NULL;
            """)
            items = cursor.fetchall()
            logger.info(f"Found {len(items)} sections to process.")
            for item_id, code, prog_id, div_id in items:
                logger.info(f"Processing section_id: {item_id}...")
                summary = get_summary(code)
                cursor.execute("UPDATE cobol_sections SET summary = %s WHERE section_id = %s;", (summary, item_id))
                vectorize_and_store(cursor, code, summary, vector_type='code', 
                                    program_id=prog_id, division_id=div_id, section_id=item_id)
        conn.commit()

        # --- 3. Process Paragraphs ---
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT par.paragraph_id, par.code, d.program_id, s.division_id, par.section_id
                FROM cobol_paragraphs par
                JOIN cobol_sections s ON par.section_id = s.section_id
                JOIN cobol_divisions d ON s.division_id = d.division_id
                WHERE par.summary IS NULL;
            """)
            items = cursor.fetchall()
            logger.info(f"Found {len(items)} paragraphs to process.")
            for item_id, code, prog_id, div_id, sec_id in items:
                logger.info(f"Processing paragraph_id: {item_id}...")
                summary = get_summary(code)
                cursor.execute("UPDATE cobol_paragraphs SET summary = %s WHERE paragraph_id = %s;", (summary, item_id))
                vectorize_and_store(cursor, code, summary, vector_type='code', 
                                    program_id=prog_id, division_id=div_id, 
                                    section_id=sec_id, paragraph_id=item_id)
        conn.commit()
        logger.info("Code processing complete.")

    except Exception as e:
        logger.info(f"Error during code processing: {e}")
        conn.rollback()

def process_call_graphs_age(conn):
    """Builds the Apache AGE graph from the raw paragraph call data."""
    logger.info("\n--- Processing Apache AGE Call Graphs ---")
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT program_id, program_name FROM cobol_programs;")
            programs = cursor.fetchall()
            
            for prog_id, prog_name in programs:
                graph_name = f"program_{prog_id}_{prog_name}".lower().replace('-', '_')
                logger.info(f"Processing graph for '{prog_name}' (Graph: {graph_name})")
                
                cursor.execute("SELECT drop_graph(%s, true);", (graph_name,))
                cursor.execute("SELECT create_graph(%s);", (graph_name,))
                
                cursor.execute("""
                    SELECT DISTINCT name FROM cobol_paragraphs par
                    JOIN cobol_sections s ON par.section_id = s.section_id
                    JOIN cobol_divisions d ON s.division_id = d.division_id
                    WHERE d.program_id = %s;
                """, (prog_id,))
                nodes = cursor.fetchall()

                logger.info(f"Creating {len(nodes)} nodes...")
                for (node_name,) in nodes:
                    cypher_query = "SELECT * FROM cypher(%s, $$ CREATE (:Paragraph {name: $name}) $$, %s) as (v agtype);"
                    params = json.dumps({"name": node_name})
                    cursor.execute(cypher_query, (graph_name, params))
                
                cursor.execute("""
                    SELECT par.name, par.calls FROM cobol_paragraphs par
                    JOIN cobol_sections s ON par.section_id = s.section_id
                    JOIN cobol_divisions d ON s.division_id = d.division_id
                    WHERE d.program_id = %s AND par.calls IS NOT NULL AND array_length(par.calls, 1) > 0;
                """, (prog_id,))
                edges_to_create = cursor.fetchall()
                
                edge_count = 0
                for caller_name, callee_list in edges_to_create:
                    for callee_name in callee_list:
                        cypher_query = """
                        SELECT * FROM cypher(%s, $$ 
                            MATCH (a:Paragraph {name: $caller}), (b:Paragraph {name: $callee}) 
                            CREATE (a)-[:CALLS]->(b) 
                        $$, %s) as (e agtype);
                        """
                        params = json.dumps({"caller": caller_name, "callee": callee_name})
                        # Handle potential missing nodes (if a PERFORM calls a non-existent paragraph)
                        try:
                            cursor.execute(cypher_query, (graph_name, params))
                            edge_count += 1
                        except Exception as age_e:
                            if "node not found" in str(age_e):
                                logger.info(f"Warning: Node '{callee_name}' (called by '{caller_name}') not found in graph. Skipping edge.")
                            else:
                                raise age_e
                
                logger.info(f"Created {edge_count} edges.")
        
        conn.commit()
        logger.info("Apache AGE graph processing complete.")
    
    except Exception as e:
        logger.info(f"Error during AGE processing: {e}")
        conn.rollback()

def process_and_vectorize_graphs(conn):
    """Summarizes and vectorizes the Mermaid call graphs."""
    logger.info("\n--- Processing Call Graph Summaries and Vectors ---")
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT d.division_id, d.call_graph_mermaid, d.program_id
                FROM cobol_divisions d
                WHERE d.name = 'PROCEDURE' AND d.call_graph_mermaid IS NOT NULL;
            """)
            graphs_to_process = cursor.fetchall()
            logger.info(f"Found {len(graphs_to_process)} procedure division graphs to process.")
            
            for div_id, mermaid_code, prog_id in graphs_to_process:
                logger.info(f"Processing graph for division_id: {div_id}...")
                summary = get_summary(mermaid_code)
                
                vectorize_and_store(cursor, mermaid_code, summary, 
                                    vector_type='graph', 
                                    program_id=prog_id, division_id=div_id)
        
        conn.commit()
        logger.info("Graph vectorization complete.")
    
    except Exception as e:
        logger.info(f"Error during graph vectorization: {e}")
        conn.rollback()

# --- Main Execution ---

def run_process():
    """Wrapper function for the main data processing pipeline."""
    setup_logging() # Ensure logging is configured
    logger.info("Starting main processing pipeline...")
    conn = get_db_connection()
    try:
        process_unsummarized_data(conn)
        process_call_graphs_age(conn)
        process_and_vectorize_graphs(conn)
    finally:
        conn.close()
    
    logger.info("All data processing complete.")


if __name__ == "__main__":
    run_process()