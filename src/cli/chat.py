import os
import sys
import logging

from src.rag.chat_engine import rag_chain
from src.core.logger import setup_logging

logger = logging.getLogger(__name__)


# --- Main Chat Application (Unchanged) ---
def main():
    logger.info("Initializing RAG chat application...")
    
    # Check if the chain failed to initialize
    if rag_chain is None:
        logger.critical("RAG chain failed to load. Check API keys and logs. Exiting.")
        print("Error: Could not initialize the RAG chain. Please check logs.", file=sys.stderr)
        sys.exit(1)
    
    # This print IS for the user
    print("Chatbot is ready. Type 'exit' or 'quit' to end.")
    
    while True:
        try:
            question = input("\nAsk about your COBOL code: ")
            if question.lower() in ["exit", "quit"]:
                logger.info("User requested to exit. Goodbye!")
                break
            if not question.strip():
                continue
            
            logger.info("\n[LLM] Generating answer...")
            answer = rag_chain.invoke(question)
            
            print("\nAnswer:")
            print(answer)
        
        except EOFError:
            break
        except KeyboardInterrupt:
            logger.info("User interrupted the session with Ctrl-C. Exiting.")
            break 
        except Exception as e:
            logger.info(f"An unexpected error occurred in the loop: {e}", exc_info=True )

if __name__ == "__main__":
    setup_logging()
    main()