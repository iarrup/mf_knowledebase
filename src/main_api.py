import logging
from fastapi import FastAPI, BackgroundTasks, status, HTTPException
from fastapi.responses import Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from src.rag.chat_engine import rag_chain

from pydantic import BaseModel
import asyncio

from src.core.logger import setup_logging
# Import pipeline triggers
from src.pipeline.db_setup import run_db_setup
from src.pipeline.ingest import run_ingest
from src.pipeline.process import run_process
from src.pipeline.generate_stories import run_generate_stories
# Import report generators
from src.reports.export import (
    get_all_user_stories_markdown,
    get_all_summaries_markdown,
    get_all_call_flows_mermaid
)

# Configure logging *before* creating the app instance
setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="COBOL Analysis API",
    description="API for ingesting, processing, and querying COBOL program data.",
    version="1.0.0"
)

# Add CORS Middleware ---
origins = [
    "http://localhost",
    "http://localhost:8501",  # Default Streamlit port
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic model for chat requets
class ChatRequest(BaseModel):
    query: str

# --- API Endpoints ---

@app.get("/", summary="API Health Check")
def get_root():
    """Simple health check endpoint."""
    return {"status": "ok", "message": "COBOL Analysis API is running."}

# --- Pipeline Triggers ---

@app.post("/api/v1/pipelines/db-setup", 
          status_code=status.HTTP_202_ACCEPTED, 
          summary="Trigger DB Setup")
def trigger_db_setup(tasks: BackgroundTasks):
    """
    Triggers the (destructive) `db_setup` pipeline in the background.
    This will drop and recreate all tables.
    """
    logger.warning("DB setup pipeline triggered via API.")
    tasks.add_task(run_db_setup)
    return {"message": "Database setup pipeline triggered in the background."}

@app.post("/api/v1/pipelines/ingest", 
          status_code=status.HTTP_202_ACCEPTED,
          summary="Trigger Raw Ingestion")
def trigger_ingest(tasks: BackgroundTasks):
    """
    Triggers the raw COBOL file ingestion (`ingest`) pipeline 
    in the background.
    """
    logger.info("Ingestion pipeline triggered via API.")
    tasks.add_task(run_ingest)
    return {"message": "Ingestion pipeline triggered in the background."}

@app.post("/api/v1/pipelines/process", 
          status_code=status.HTTP_202_ACCEPTED,
          summary="Trigger Data Processing")
def trigger_process(tasks: BackgroundTasks):
    """
    Triggers the main data processing (`process`) pipeline 
    (Summaries, AGE graphs, Vectors) in the background.
    """
    logger.info("Processing pipeline triggered via API.")
    tasks.add_task(run_process)
    return {"message": "Data processing pipeline triggered in the background."}

@app.post("/api/v1/pipelines/generate-stories", 
          status_code=status.HTTP_202_ACCEPTED,
          summary="Trigger User Story Generation")
def trigger_generate_stories(tasks: BackgroundTasks):
    """
    Triggers the user story generation (`generate_stories`) pipeline 
    in the background.
    """
    logger.info("User story generation pipeline triggered via API.")
    tasks.add_task(run_generate_stories)
    return {"message": "User story generation pipeline triggered in the background."}

# --- Report Retrieval ---

@app.get("/api/v1/reports/user-stories", 
         summary="Get User Stories Report", 
         response_class=Response)
def get_stories_report():
    """
    Retrieves a consolidated Markdown report of all generated user stories.
    """
    logger.info("User stories report requested via API.")
    md_content = get_all_user_stories_markdown()
    return Response(content=md_content, media_type="text/markdown")

@app.get("/api/v1/reports/summaries", 
         summary="Get Code Summaries Report", 
         response_class=Response)
def get_summaries_report():
    """
    Retrieves a hierarchical Markdown report of all code summaries 
    (Program, Division, Section, Paragraph).
    """
    logger.info("Summaries report requested via API.")
    md_content = get_all_summaries_markdown()
    return Response(content=md_content, media_type="text/markdown")

@app.get("/api/v1/reports/call-graphs", 
         summary="Get Call Graphs Report", 
         response_class=Response)
def get_graphs_report():
    """
    Retrieves a Markdown report containing all Procedure Division 
    call graphs in Mermaid format.
    """
    logger.info("Call graphs report requested via API.")
    md_content = get_all_call_flows_mermaid()
    return Response(content=md_content, media_type="text/markdown")


# --- NEW: Chat Endpoint ---
@app.post("/api/v1/chat/query", summary="Query the COBOL knowledge base")
async def chat_query(request: ChatRequest):
    """
    Receives a user query, streams the RAG response back.
    """
    # Check if chain is loaded
    if rag_chain is None:
        logger.error("Chat query received, but RAG chain is not loaded.")
        raise HTTPException(
            status_code=503, # Service Unavailable
            detail="Chat engine is not available. Check logs for initialization errors (e.g., API keys)."
        )
        
    logger.info(f"API chat query received: {request.query}")
    
    async def stream_generator():
        try:
            # rag_chain.stream() is an async generator
            async for chunk in rag_chain.astream(request.query):
                yield chunk
        except Exception as e:
            logger.error(f"Error during RAG stream: {e}", exc_info=True)
            yield f"Error: Could not process query. {e}"

    return StreamingResponse(stream_generator(), media_type="text/plain")
