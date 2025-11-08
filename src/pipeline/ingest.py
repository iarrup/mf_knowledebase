import os
import sys
import logging
from pathlib import Path
import psycopg2

from src.core.config import settings
from src.core.models import Program
from src.analysis.cobol_parser import parse_program
from src.core.logger import setup_logging

from src.pipeline.data_loader import load_data_source

logger = logging.getLogger(__name__)

# --- Configuration ---
COBOL_EXTENSIONS = {".cbl", ".cob", ".CBL", ".COB"}

# --- Database Functions ---
def get_db_connection():
    """Establishes a simple psycopg2 database connection."""
    try:
        conn = psycopg2.connect(settings.DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        logger.info(f"FATAL: Could not connect to database: {e}")
        sys.exit(1)

# --- Main Ingestion Logic ---

def run_ingest():
    """
    Orchestrates the RAW parsing and ingestion.
    Does NOT summarize or vectorize.
    """
    setup_logging()
    logger.info("Starting RAW COBOL ingestion pipeline...")

    try:
        cobol_dir = load_data_source()
        logger.info(f"Data source loaded. Reading from: {cobol_dir}")
    except Exception as e:
        logger.critical(f"Failed to load data source: {e}", exc_info=True)
        sys.exit(1)   
         
    files_to_process = [
        p for p in cobol_dir.rglob('*') 
        if p.is_file() and p.suffix in COBOL_EXTENSIONS
    ]

    if not files_to_process:
        logger.info(f"No COBOL files found in {cobol_dir}. Exiting.")
        return

    logger.info(f"Found {len(files_to_process)} COBOL files.")
    conn = get_db_connection()

    for file_path in files_to_process:
        filename = str(file_path.relative_to(COBOL_DIR))
        logger.info(f"\n--- Processing: {filename} ---")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            logger.info("Parsing COBOL structure...")
            program: Program = parse_program(filename, content)
            
            with conn.cursor() as cursor:
                logger.info(f"Checking for existing program '{program.program_name}'...")
                cursor.execute(
                    "DELETE FROM cobol_programs WHERE filename = %s;",
                    (program.filename,)
                )
                
                cursor.execute(
                    """
                    INSERT INTO cobol_programs (filename, program_name, content)
                    VALUES (%s, %s, %s)
                    RETURNING program_id;
                    """,
                    (program.filename, program.program_name, program.content)
                )
                program_id = cursor.fetchone()[0]
                logger.info(f"Ingested raw program with program_id: {program_id}")
                
                for div in program.divisions:
                    cursor.execute(
                        """
                        INSERT INTO cobol_divisions (program_id, name, code, summary, call_graph_mermaid)
                        VALUES (%s, %s, %s, %s, %s) RETURNING division_id;
                        """,
                        (program_id, div.name, div.code, None, div.call_graph_mermaid)
                    )
                    division_id = cursor.fetchone()[0]
                    
                    for sec in div.sections:
                        cursor.execute(
                            """
                            INSERT INTO cobol_sections (division_id, name, code, summary)
                            VALUES (%s, %s, %s, %s) RETURNING section_id;
                            """,
                            (division_id, sec.name, sec.code, None)
                        )
                        section_id = cursor.fetchone()[0]
                        
                        if div.name == "PROCEDURE":
                            for para in sec.paragraphs:
                                cursor.execute(
                                    """
                                    INSERT INTO cobol_paragraphs (section_id, name, code, summary, calls)
                                    VALUES (%s, %s, %s, %s, %s) RETURNING paragraph_id;
                                    """,
                                    (section_id, para.name, para.code, None, para.calls) # Add calls list
                                )
            
            conn.commit()
            logger.info(f"Successfully ingested raw structure for '{filename}'.")

        except Exception as e:
            logger.info(f"ERROR processing file {filename}: {e}")
            conn.rollback()

    conn.close()
    logger.info("\nRaw ingestion pipeline complete.")


if __name__ == "__main__":
    run_ingest()