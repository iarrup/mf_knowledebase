import os
import sys
import logging
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import OpenAIEmbeddings
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel, Field, ConfigDict
from typing import List
import psycopg2


from src.core.config import settings

logger = logging.getLogger(__name__)

# --- Configuration ---
# Set keys here for any module that imports this file
os.environ["OPENAI_API_KEY"] = settings.OPENAI_API_KEY
os.environ["GOOGLE_API_KEY"] = settings.GOOGLE_API_KEY
EMBEDDING_MODEL = "text-embedding-3-small"
LLM_MODEL = "gemini-2.5-pro"


# --- RAG Prompt Template (MOVED HERE) ---
RAG_PROMPT_TEMPLATE = """
You are an expert COBOL programmer and business analyst. Answer the user's question based *only* on the provided context.
The context contains structured COBOL code, graph diagrams, summaries, and user stories.
Use all this information to be precise in your answer.

- If the context `Type` is 'code', use the COBOL code and its summary.
- If the context `Type` is 'graph', use the MermaidJS graph data to explain the call flow.
- If the context `Type` is 'user_story', use this to understand the business requirement or capability.

If the context does not contain the answer, state that you don't have enough information.

Structured Context:
---
{context}
---

Question:
{question}
"""

# --- Custom LangChain Retriever (MOVED HERE) ---
class StructuredCobolRetriever(BaseRetriever, BaseModel):
    """
    A custom retriever that queries the structured cobol_vectors table
    and joins with other tables to build a rich context.
    """
    embeddings: OpenAIEmbeddings = Field(..., exclude=True) 
    db_url: str
    k: int = 5
    
    model_config = ConfigDict(arbitrary_types_allowed=True) 

    def _get_relevant_documents(self, query: str) -> List[Document]:
        """
        The core logic for retrieving documents.
        """
        logger.info(f"Retrieving relevant code for query: '{query}'")
        
        query_embedding = self.embeddings.embed_query(query)
        pgvector_string = "[" + ",".join(map(str, query_embedding)) + "]"
        
        sql_query = """
        SELECT 
            v.content, v.summary, v.vector_type,
            p.program_name, d.name as division_name,
            s.name as section_name, par.name as paragraph_name
        FROM cobol_vectors v
        LEFT JOIN cobol_programs p ON v.program_id = p.program_id
        LEFT JOIN cobol_divisions d ON v.division_id = d.division_id
        LEFT JOIN cobol_sections s ON v.section_id = s.section_id
        LEFT JOIN cobol_paragraphs par ON v.paragraph_id = par.paragraph_id
        ORDER BY v.embedding <-> %s
        LIMIT %s;
        """
        
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            docs = []
            with conn.cursor() as cursor:
                cursor.execute(sql_query, (pgvector_string, self.k))
                results = cursor.fetchall()
            
            logger.info(f"Found {len(results)} relevant structured chunks.")

            for row in results:
                content, summary, vector_type, prog, div, sec, para = row
                
                location = f"Program: {prog}"
                if div: location += f" > Division: {div}"
                if sec: location += f" > Section: {sec}"
                if para: location += f" > Paragraph: {para}"
                
                page_content = f"""
                    Location: {location}
                    Type: {vector_type}
                    Title/Summary: {summary}
                    Content:
                    {content}
                """
                docs.append(Document(page_content=page_content))
                
            return docs

        except Exception as e:
            logger.error(f"Error during vector search: {e}", exc_info=True)
            return []
        finally:
            if conn:
                conn.close()

# --- Helper Function (MOVED HERE) ---
def format_docs(docs: List[Document]) -> str:
    """Joins retrieved documents into a single string for the prompt."""
    return "\n\n====================\n\n".join(doc.page_content for doc in docs)


# --- Re-usable Chat Engine ---

def get_rag_chain():
    """
    Initializes and returns a runnable RAG chain.
    """
    logger.info("Initializing RAG chain...")
    try:
        embeddings = OpenAIEmbeddings(model=os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small"))
        llm = ChatGoogleGenerativeAI(model=os.environ.get("LLM_MODEL", "gemini-2.5-pro"))
    except Exception as e:
        logger.critical(f"Error initializing models in chat_engine: {e}", exc_info=True)
        sys.exit(1)

    retriever = StructuredCobolRetriever(
        embeddings=embeddings, 
        db_url=settings.DATABASE_URL,
        k=5
    )
    
    prompt = ChatPromptTemplate.from_template(RAG_PROMPT_TEMPLATE)

    rag_chain = (
        {"context": retriever | format_docs, "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )
    logger.info("RAG chain initialization complete.")
    return rag_chain

# Create a single, re-usable instance

# Create a single, re-usable instance
# This can fail if keys aren't set, so we wrap it
try:
    rag_chain = get_rag_chain()
except Exception as e:
    logger.critical(f"Failed to create rag_chain on startup: {e}", exc_info=True)
    rag_chain = None