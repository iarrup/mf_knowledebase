import os
import logging
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser, JsonOutputParser
from pydantic import BaseModel # NEW
from src.core.config import settings

logger = logging.getLogger(__name__)

# Initialize LLM
os.environ["GOOGLE_API_KEY"] = settings.GOOGLE_API_KEY
llm = ChatGoogleGenerativeAI(model="gemini-2.5-pro")

# --- 1. Code Summarization ---

SUMMARY_PROMPT_TEMPLATE = """
You are an expert COBOL programmer. Summarize the following COBOL code snippet.
Focus on its primary purpose, inputs, and outputs.
Be concise and clear.

COBOL Code:
---
{code}
---

Summary:
"""
SUMMARY_PROMPT = ChatPromptTemplate.from_template(SUMMARY_PROMPT_TEMPLATE)
summarize_chain = SUMMARY_PROMPT | llm | StrOutputParser()

def get_summary(code_snippet: str) -> str:
    """Generates a summary for a given COBOL code snippet."""
    if not code_snippet.strip():
        return "N/A - No code provided."
        
    logger.info(f"Summarizing {len(code_snippet)} bytes of code...")
    try:
        summary = summarize_chain.invoke({"code": code_snippet})
        logger.info("...Summary generated.")
        return summary
    except Exception as e:
        logger.info(f"Error during summarization: {e}")
        return f"Error: Could not generate summary. {e}"

# --- 2. User Story Generation (NEW) ---

# Pydantic model for the JSON output
class UserStory(BaseModel):
    title: str
    story_text: str

story_parser = JsonOutputParser(pydantic_object=UserStory)

USER_STORY_PROMPT_TEMPLATE = """
You are a senior business analyst expert at reverse-engineering legacy COBOL systems.
Your task is to generate a single, high-quality user story in the "Connextra" format (As a [persona], I want [goal], so that [benefit]) based on the provided technical summary of a COBOL code component.

RULES:
1.  Persona: Be specific (e.g., "data entry clerk", "system administrator", "batch process", "report consumer").
2.  Goal: Clearly state the action or capability described in the summary.
3.  Benefit: Clearly state the business value or outcome.
4.  Format: Respond *only* with the JSON object as specified by the format instructions.

TECHNICAL SUMMARY:
---
{summary}
---

{format_instructions}
"""

USER_STORY_PROMPT = ChatPromptTemplate.from_template(
    USER_STORY_PROMPT_TEMPLATE,
    partial_variables={"format_instructions": story_parser.get_format_instructions()}
)

# This is the chain you will import into the new script
generate_story_chain = USER_STORY_PROMPT | llm | story_parser