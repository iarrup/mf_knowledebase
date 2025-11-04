import psycopg2
import logging
from src.core.config import settings
from src.core.logger import setup_logging

# Configure logger
logger = logging.getLogger(__name__)

EMBEDDING_DIM = 1536

def create_tables():
    """Connects to the DB and creates the new structured schema."""
    logger.info("Connecting to the database...")
    conn = None
    try:
        with psycopg2.connect(settings.DATABASE_URL) as conn:
            with conn.cursor() as cursor:
                
                logger.info("Dropping old tables (if they exist)...")
                cursor.execute("DROP TABLE IF EXISTS cobol_vectors CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS cobol_paragraphs CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS cobol_sections CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS cobol_divisions CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS cobol_programs CASCADE;")

                # 1. cobol_programs
                cursor.execute("""
                CREATE TABLE cobol_programs (
                    program_id SERIAL PRIMARY KEY,
                    filename VARCHAR(255) NOT NULL UNIQUE,
                    program_name VARCHAR(255),
                    content TEXT NOT NULL,
                    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                );
                """)
                logger.info("Table 'cobol_programs' created.")

                # 2. cobol_divisions
                cursor.execute("""
                CREATE TABLE cobol_divisions (
                    division_id SERIAL PRIMARY KEY,
                    program_id INTEGER NOT NULL REFERENCES cobol_programs(program_id) ON DELETE CASCADE,
                    name VARCHAR(255) NOT NULL,
                    code TEXT NOT NULL,
                    summary TEXT,
                    call_graph_mermaid TEXT 
                );
                """)
                logger.info("Table 'cobol_divisions' created.")

                # 3. cobol_sections
                cursor.execute("""
                CREATE TABLE cobol_sections (
                    section_id SERIAL PRIMARY KEY,
                    division_id INTEGER NOT NULL REFERENCES cobol_divisions(division_id) ON DELETE CASCADE,
                    name VARCHAR(255) NOT NULL,
                    code TEXT NOT NULL,
                    summary TEXT
                );
                """)
                logger.info("Table 'cobol_sections' created.")

                # 4. cobol_paragraphs
                cursor.execute("""
                CREATE TABLE cobol_paragraphs (
                    paragraph_id SERIAL PRIMARY KEY,
                    section_id INTEGER NOT NULL REFERENCES cobol_sections(section_id) ON DELETE CASCADE,
                    name VARCHAR(255) NOT NULL,
                    code TEXT NOT NULL,
                    summary TEXT,
                    calls TEXT[] 
                );
                """)
                logger.info("Table 'cobol_paragraphs' created.")

                # 5. cobol_vectors
                cursor.execute(f"""
                CREATE TABLE cobol_vectors (
                    vector_id SERIAL PRIMARY KEY,
                    content TEXT NOT NULL,
                    summary TEXT,
                    embedding vector({EMBEDDING_DIM}),
                    vector_type VARCHAR(50) DEFAULT 'code', 
                    
                    program_id INTEGER REFERENCES cobol_programs(program_id) ON DELETE CASCADE,
                    division_id INTEGER REFERENCES cobol_divisions(division_id) ON DELETE CASCADE,
                    section_id INTEGER REFERENCES cobol_sections(section_id) ON DELETE CASCADE,
                    paragraph_id INTEGER REFERENCES cobol_paragraphs(paragraph_id) ON DELETE CASCADE,
                    
                    CONSTRAINT chk_vector_source CHECK (
                        program_id IS NOT NULL OR
                        division_id IS NOT NULL OR
                        section_id IS NOT NULL OR
                        paragraph_id IS NOT NULL
                    )
                );
                """)
                logger.info("Table 'cobol_vectors' created.")
                
                # 6. HNSW index
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_hnsw_embedding
                ON cobol_vectors
                USING hnsw (embedding vector_l2_ops);
                """)
                logger.info("HNSW index created on 'cobol_vectors'.")

            conn.commit()
            logger.info("Database setup complete.")

    except (Exception, psycopg2.DatabaseError) as e:
        logger.info(f"Error setting up database: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
    
    
def run_db_setup():
    """Wrapper function to be called by API or script."""
    setup_logging() # Ensure logging is configured
    logger.info("Starting DB setup pipeline...")
    create_tables()
    logger.info("DB setup pipeline finished.")
    
    
if __name__ == "__main__":
    run_db_setup()
    