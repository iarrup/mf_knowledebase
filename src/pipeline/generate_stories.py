import sys
import logging
from src.core.logger import setup_logging
from src.database.vector_utils import get_db_connection, vectorize_and_store
from src.analysis.summarizer import generate_story_chain, UserStory

logger = logging.getLogger(__name__)

def generate_stories_for_component(conn, table_name, id_column, summary_column, fk_map):
    """
    Generic function to generate and store user stories for any component
    (program, division, section, paragraph).
    """
    logger.info(f"\n--- Generating User Stories for: {table_name} ---")
    
    # Construct query to get all components with their summaries
    # We also join up to the program_id for linking
    query_base = f"""
        SELECT t.{id_column}, t.{summary_column}, d.program_id, s.division_id, par.section_id
        FROM {table_name} t
        LEFT JOIN cobol_divisions d ON t.division_id = d.division_id
        LEFT JOIN cobol_sections s ON t.section_id = s.section_id
        LEFT JOIN cobol_paragraphs par ON t.paragraph_id = par.paragraph_id
        WHERE t.{summary_column} IS NOT NULL AND t.{summary_column} NOT LIKE 'Error:%'
    """
    # This logic is a bit complex, but handles all table hierarchies
    if table_name == 'cobol_sections':
        query_base = f"""
            SELECT s.{id_column}, s.{summary_column}, d.program_id, s.division_id, NULL as section_id
            FROM cobol_sections s
            JOIN cobol_divisions d ON s.division_id = d.division_id
            WHERE s.{summary_column} IS NOT NULL AND s.{summary_column} NOT LIKE 'Error:%'
        """
    elif table_name == 'cobol_divisions':
         query_base = f"""
            SELECT d.{id_column}, d.{summary_column}, d.program_id, d.division_id, NULL as section_id
            FROM cobol_divisions d
            WHERE d.{summary_column} IS NOT NULL AND d.{summary_column} NOT LIKE 'Error:%'
        """
    elif table_name == 'cobol_paragraphs':
        query_base = f"""
            SELECT p.{id_column}, p.{summary_column}, d.program_id, s.division_id, p.section_id
            FROM cobol_paragraphs p
            JOIN cobol_sections s ON p.section_id = s.section_id
            JOIN cobol_divisions d ON s.division_id = d.division_id
            WHERE p.{summary_column} IS NOT NULL AND p.{summary_column} NOT LIKE 'Error:%'
        """
    
    stories_created = 0
    try:
        with conn.cursor() as cursor:
            cursor.execute(query_base)
            items = cursor.fetchall()
            logger.info(f"Found {len(items)} summarized items to process.")
            
            for item_id, summary, prog_id, div_id, sec_id in items:
                logger.info(f"Generating story for {id_column}: {item_id}...")
                
                # 1. Generate User Story
                try:
                    story_data = generate_story_chain.invoke({"summary": summary})
                    story = UserStory.model_validate(story_data)
                except Exception as llm_e:
                    logger.info(f"  Error generating story (LLM or JSON parse failed): {llm_e}")
                    continue # Skip this item
                
                # 2. Store as a new vector
                fk_ids = {
                    "program_id": prog_id,
                    "division_id": div_id,
                    "section_id": sec_id,
                    "paragraph_id": item_id if table_name == 'cobol_paragraphs' else None
                }

                # Use the helper to create the vector
                created = vectorize_and_store(
                    cursor=cursor,
                    text_content=story.story_text,
                    summary=story.title,
                    vector_type="user_story",
                    **fk_ids
                )
                if created:
                    stories_created += 1
        
        conn.commit()
        logger.info(f"Successfully created {stories_created} new user stories for {table_name}.")

    except Exception as e:
        logger.info(f"Error processing {table_name}: {e}")
        conn.rollback()

def generate_stories_for_graphs(conn):
    """Generates user stories for call graphs."""
    logger.info("\n--- Generating User Stories for: Call Graphs ---")
    stories_created = 0
    try:
        with conn.cursor() as cursor:
            # We query the vector table for 'graph' type vectors
            cursor.execute("""
                SELECT content, summary, program_id, division_id
                FROM cobol_vectors
                WHERE vector_type = 'graph'
            """)
            items = cursor.fetchall()
            logger.info(f"Found {len(items)} graph items to process.")

            for mermaid_code, summary, prog_id, div_id in items:
                logger.info(f"Generating story for graph (division_id: {div_id})...")
                
                # 1. Generate User Story
                try:
                    # We combine the summary and code for richer context
                    context = f"Summary: {summary}\n\nMermaid Graph:\n{mermaid_code}"
                    story_data = generate_story_chain.invoke({"summary": context})
                    story = UserStory.model_validate(story_data)
                except Exception as llm_e:
                    logger.info(f"  Error generating story (LLM or JSON parse failed): {llm_e}")
                    continue
                
                # 2. Store as a new vector
                created = vectorize_and_store(
                    cursor=cursor,
                    text_content=story.story_text,
                    summary=story.title,
                    vector_type="user_story",
                    program_id=prog_id,
                    division_id=div_id
                )
                if created:
                    stories_created += 1
        
        conn.commit()
        logger.info(f"Successfully created {stories_created} new user stories for graphs.")

    except Exception as e:
        logger.info(f"Error processing graphs: {e}")
        conn.rollback()


def run_generate_stories():
    """Wrapper function for the user story generation pipeline."""
    setup_logging() # Ensure logging is configured
    logger.info("Starting user story generation pipeline...")
    
    conn = get_db_connection()
    
    try:
        # Note: The order doesn't really matter here
        generate_stories_for_component(conn, "cobol_divisions", "division_id", "summary", 
                                       fk_map={"division_id": "division_id", "program_id": "program_id"})
        
        generate_stories_for_component(conn, "cobol_sections", "section_id", "summary", 
                                       fk_map={"section_id": "section_id", "division_id": "division_id"})
        
        generate_stories_for_component(conn, "cobol_paragraphs", "paragraph_id", "summary", 
                                       fk_map={"paragraph_id": "paragraph_id", "section_id": "section_id"})
        
        generate_stories_for_graphs(conn)
        
    finally:
        conn.close()
    
    logger.info("\nAll user story generation complete.")

if __name__ == "__main__":
    run_generate_stories()