import os
import sys
from pathlib import Path
import psycopg
# REMOVED: from psycopg.extras import execute_batch

from langchain_openai import OpenAIEmbeddings
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_text_splitters import RecursiveCharacterTextSplitter, Language

from src.config import settings

# --- Configuration ---

# Set API keys in the environment for LangChain
os.environ["OPENAI_API_KEY"] = settings.OPENAI_API_KEY
os.environ["GOOGLE_API_KEY"] = settings.GOOGLE_API_KEY

# Source code directory
COBOL_DIR = Path("data/input/prog")
COBOL_EXTENSIONS = {".cbl", ".cob", ".CBL", ".COB"}

# Embedding model
EMBEDDING_MODEL = "text-embedding-3-small"

# LLM
LLM_MODEL = "gemini-2.5-pro"

# --- LangChain Components ---

def get_db_connection():
    """Establishes and returns a database connection."""
    print("Connecting to the database...")
    try:
        conn = psycopg.connect(settings.DATABASE_URL)
        return conn
    except psycopg.OperationalError as e:
        print(f"FATAL: Could not connect to database at {settings.DATABASE_URL}")
        print("Please ensure the PostgreSQL database is running.")
        sys.exit(1)

def get_langchain_components():
    """Initializes and returns LangChain components."""
    print(f"Initializing embedding model: {EMBEDDING_MODEL}")
    embeddings = OpenAIEmbeddings(model=EMBEDDING_MODEL)

    print(f"Initializing LLM: {LLM_MODEL}")
    llm = ChatGoogleGenerativeAI(model=LLM_MODEL)

    # COBOL-aware text splitter
    cobol_splitter = RecursiveCharacterTextSplitter.from_language(
        language=Language.COBOL,
        chunk_size=2000,
        chunk_overlap=200
    )
    return embeddings, llm, cobol_splitter

def ingest_cobol_files():
    """
    Reads COBOL files, ingests raw code, creates embeddings,
    and stores everything in PostgreSQL.
    """
    print("Starting COBOL ingestion process...")
    embeddings, llm, cobol_splitter = get_langchain_components()

    files_to_process = [
        p for p in COBOL_DIR.rglob('*') 
        if p.is_file() and p.suffix in COBOL_EXTENSIONS
    ]

    if not files_to_process:
        print(f"No COBOL files found in {COBOL_DIR}. Exiting.")
        return

    print(f"Found {len(files_to_process)} COBOL files to process.")

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            for file_path in files_to_process:
                filename = str(file_path.relative_to(COBOL_DIR))
                print(f"\n--- Processing: {filename} ---")
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()

                    # 1. Ingest the raw program file
                    # Use ON CONFLICT to update content if file already exists
                    cursor.execute("""
                        INSERT INTO cobol_programs (filename, content)
                        VALUES (%s, %s)
                        ON CONFLICT (filename) DO UPDATE
                        SET content = EXCLUDED.content, ingested_at = CURRENT_TIMESTAMP
                        RETURNING id;
                    """, (filename, content))
                    
                    program_id = cursor.fetchone()[0]
                    print(f"Ingested raw file '{filename}' with program_id: {program_id}")

                    # 2. Before adding new vectors, clear old ones for this program
                    cursor.execute(
                        "DELETE FROM cobol_vectors WHERE program_id = %s",
                        (program_id,)
                    )
                    print("Cleared old vector chunks for this program.")

                    # 3. Split the code into chunks
                    chunks = cobol_splitter.split_text(content)
                    if not chunks:
                        print("Warning: No text chunks were generated for this file.")
                        continue
                        
                    print(f"Split code into {len(chunks)} chunks.")

                    # 4. Create embeddings for all chunks
                    print("Generating embeddings...")
                    chunk_embeddings = embeddings.embed_documents(chunks)
                    print("Embeddings generated.")

                    # 5. Store chunks and embeddings in the database using cursor.executemany()
                    data_to_insert = [
                        (program_id, chunk, embedding)
                        for chunk, embedding in zip(chunks, chunk_embeddings)
                    ]
                    
                    # Using cursor.executemany() for efficient batch insertion
                    cursor.executemany("""
                        INSERT INTO cobol_vectors (program_id, chunk_text, embedding)
                        VALUES (%s, %s, %s)
                    """, data_to_insert)

                    print(f"Successfully inserted {len(data_to_insert)} vector chunks into database.")
                    conn.commit()

                except Exception as e:
                    print(f"ERROR processing file {filename}: {e}")
                    conn.rollback() # Rollback changes for this file

    print("\nIngestion process complete.")

if __name__ == "__main__":
    ingest_cobol_files()