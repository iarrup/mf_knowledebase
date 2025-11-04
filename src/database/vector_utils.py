import os
import sys
import logging
import psycopg2
from pgvector.psycopg2 import register_vector
from langchain_openai import OpenAIEmbeddings

from src.core.config import settings

logger = logging.getLogger(__name__)

os.environ["OPENAI_API_KEY"] = settings.OPENAI_API_KEY

# Initialize embedding model (only once)
try:
    embeddings_model = OpenAIEmbeddings(model="text-embedding-3-small")
except Exception as e:
    logger.info(f"Failed to initialize embeddings model: {e}")
    sys.exit(1)

def get_db_connection():
    """Establishes a psycopg2 connection and registers the vector type."""
    try:
        conn = psycopg2.connect(settings.DATABASE_URL)
        register_vector(conn)
        # Set search_path for Apache AGE
        with conn.cursor() as cursor:
            cursor.execute("SET search_path = ag_catalog, '$user', public;")
        return conn
    except psycopg2.OperationalError as e:
        logger.info(f"FATAL: Could not connect to database: {e}")
        sys.exit(1)

def vectorize_and_store(cursor, text_content, summary, vector_type, **fk_ids):
    """
    Generates embedding and stores it in the cobol_vectors table.
    Checks for existence before inserting to avoid duplicates.
    """
    
    # 1. Check for existing vector to avoid duplicates
    check_query_parts = ["SELECT 1 FROM cobol_vectors WHERE vector_type = %s"]
    check_values = [vector_type]
    
    fk_key_map = {
        "program_id": "program_id",
        "division_id": "division_id",
        "section_id": "section_id",
        "paragraph_id": "paragraph_id"
    }

    for key, col_name in fk_key_map.items():
        if key in fk_ids and fk_ids[key] is not None:
            check_query_parts.append(f"{col_name} = %s")
            check_values.append(fk_ids[key])
        else:
            # Handle cases where the key is present but None (e.g., for program-level)
            check_query_parts.append(f"{col_name} IS NULL")

    cursor.execute(" AND ".join(check_query_parts), tuple(check_values))
    if cursor.fetchone():
        logger.info(f"Vector type '{vector_type}' for {fk_ids} already exists. Skipping.")
        return False # Return False to indicate skipped

    # --- 2. If not found, create it ---
    logger.info(f"Creating vector type '{vector_type}' for {fk_ids}...")
    # The combined text is what gets vectorized
    combined_text = f"Content Type: {vector_type}\nTitle: {summary}\nContent: {text_content}"
    
    embedding = embeddings_model.embed_query(combined_text)
    
    columns = ["content", "summary", "embedding", "vector_type"]
    values = [text_content, summary, embedding, vector_type]
    
    for key, col_name in fk_key_map.items():
        if key in fk_ids:
            columns.append(col_name)
            values.append(fk_ids[key])

    placeholders = ", ".join(["%s"] * len(values))
    column_names = ", ".join(columns)
    
    # 3. Insert into database
    insert_query = f"INSERT INTO cobol_vectors ({column_names}) VALUES ({placeholders})"
    cursor.execute(insert_query, tuple(values))
    return True # Return True to indicate creation