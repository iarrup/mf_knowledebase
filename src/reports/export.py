import sys
import logging
import argparse
import os
from pathlib import Path

from src.core.logger import setup_logging
from src.database.vector_utils import get_db_connection

logger = logging.getLogger(__name__)

# --- Internal Core Logic Functions (require a cursor) ---

def _get_all_user_stories_markdown(cursor) -> str:
    """
    Internal: Fetches all user stories and formats them as Markdown.
    """
    logger.info("Fetching user stories...")
    query = """
    SELECT 
        v.summary as title,
        v.content as story_text,
        p.program_name,
        d.name as division_name,
        s.name as section_name,
        par.name as paragraph_name
    FROM 
        cobol_vectors v
    LEFT JOIN 
        cobol_programs p ON v.program_id = p.program_id
    LEFT JOIN 
        cobol_divisions d ON v.division_id = d.division_id
    LEFT JOIN 
        cobol_sections s ON v.section_id = s.section_id
    LEFT JOIN 
        cobol_paragraphs par ON v.paragraph_id = par.paragraph_id
    WHERE 
        v.vector_type = 'user_story'
    ORDER BY
        p.program_name, d.division_id, s.section_id, par.paragraph_id;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    
    logger.info(f"Found {len(results)} user stories.")
    
    md_lines = ["# COBOL User Stories\n"]
    current_program = ""

    for title, story_text, prog, div, sec, para in results:
        if prog != current_program:
            md_lines.append(f"\n## Program: {prog}\n")
            current_program = prog
        
        location_parts = []
        if div: location_parts.append(div)
        if sec: location_parts.append(sec)
        if para: location_parts.append(para)
        location = " > ".join(location_parts)
        
        md_lines.append(f"### {title}")
        md_lines.append(f"_{story_text}_")
        md_lines.append(f"**(Source: {location})**")
        md_lines.append("\n---\n")

    return "\n".join(md_lines)

def _get_all_summaries_markdown(cursor) -> str:
    """
    Internal: Fetches all code summaries and formats them as hierarchical Markdown.
    """
    logger.info("Fetching hierarchical code summaries...")
    md_lines = ["# COBOL Code Summaries\n"]

    cursor.execute("SELECT program_id, program_name FROM cobol_programs ORDER BY program_name;")
    programs = cursor.fetchall()
    
    for prog_id, prog_name in programs:
        md_lines.append(f"\n## Program: {prog_name}\n")
        
        cursor.execute(
            "SELECT division_id, name, summary FROM cobol_divisions WHERE program_id = %s ORDER BY division_id;",
            (prog_id,)
        )
        divisions = cursor.fetchall()
        
        for div_id, div_name, div_summary in divisions:
            md_lines.append(f"### Division: {div_name}")
            md_lines.append(f"{div_summary or 'No summary available.'}\n")
            
            cursor.execute(
                "SELECT section_id, name, summary FROM cobol_sections WHERE division_id = %s ORDER BY section_id;",
                (div_id,)
            )
            sections = cursor.fetchall()
            
            for sec_id, sec_name, sec_summary in sections:
                md_lines.append(f"#### Section: {sec_name}")
                md_lines.append(f"{sec_summary or 'No summary available.'}\n")
                
                if div_name == "PROCEDURE":
                    cursor.execute(
                        "SELECT name, summary FROM cobol_paragraphs WHERE section_id = %s ORDER BY paragraph_id;",
                        (sec_id,)
                    )
                    paragraphs = cursor.fetchall()
                    
                    for para_name, para_summary in paragraphs:
                        md_lines.append(f"##### Paragraph: {para_name}")
                        md_lines.append(f"{para_summary or 'No summary available.'}\n")

    return "\n".join(md_lines)

def _get_all_call_flows_mermaid(cursor) -> str:
    """
    Internal: Fetches all call graphs and formats them as Mermaid Markdown.
    """
    logger.info("Fetching call flow graphs...")
    query = """
    SELECT 
        p.program_name,
        d.call_graph_mermaid
    FROM 
        cobol_divisions d
    JOIN 
        cobol_programs p ON d.program_id = p.program_id
    WHERE 
        d.name = 'PROCEDURE' AND d.call_graph_mermaid IS NOT NULL
    ORDER BY
        p.program_name;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    
    logger.info(f"Found {len(results)} call graphs.")
    
    md_lines = ["# COBOL Program Call Flows\n"]
    
    for prog_name, mermaid_code in results:
        md_lines.append(f"\n## Program: {prog_name}\n")
        md_lines.append("```mermaid")
        md_lines.append(mermaid_code)
        md_lines.append("```\n")
        md_lines.append("\n---\n")
        
    return "\n".join(md_lines)

# --- Public API Functions (Importable) ---

def get_all_user_stories_markdown() -> str:
    """
    Public: Connects to DB, fetches all user stories, and returns as Markdown.
    """
    logger.info("Public export function called: get_all_user_stories_markdown")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            return _get_all_user_stories_markdown(cursor)
    except Exception as e:
        logger.error(f"Error in get_all_user_stories_markdown: {e}", exc_info=True)
        return "# Error: Could not generate user story report."
    finally:
        if conn:
            conn.close()

def get_all_summaries_markdown() -> str:
    """
    Public: Connects to DB, fetches all summaries, and returns as Markdown.
    """
    logger.info("Public export function called: get_all_summaries_markdown")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            return _get_all_summaries_markdown(cursor)
    except Exception as e:
        logger.error(f"Error in get_all_summaries_markdown: {e}", exc_info=True)
        return "# Error: Could not generate summaries report."
    finally:
        if conn:
            conn.close()

def get_all_call_flows_mermaid() -> str:
    """
    Public: Connects to DB, fetches all call flows, and returns as Mermaid Markdown.
    """
    logger.info("Public export function called: get_all_call_flows_mermaid")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            return _get_all_call_flows_mermaid(cursor)
    except Exception as e:
        logger.error(f"Error in get_all_call_flows_mermaid: {e}", exc_info=True)
        return "# Error: Could not generate call flow report."
    finally:
        if conn:
            conn.close()

# --- Main CLI (Updated to use internal functions) ---

def main():
    """
    Main entry point for the export CLI.
    """
    parser = argparse.ArgumentParser(description="Export COBOL analysis data from the database.")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--stories", action="store_true", help="Export all User Stories.")
    group.add_argument("--summaries", action="store_true", help="Export all code summaries.")
    group.add_argument("--graphs", action="store_true", help="Export all call flow graphs.")
    
    parser.add_argument(
        "-o", "--output", type=str, required=True,
        help="The path to the output file (e.g., 'reports/stories.md')."
    )
    
    args = parser.parse_args()
    
    conn = None
    try:
        # The CLI main function manages its own connection
        conn = get_db_connection()
        with conn.cursor() as cursor:
            
            output_content = ""
            
            if args.stories:
                logger.info("Generating User Stories report for CLI...")
                # Call the internal function
                output_content = _get_all_user_stories_markdown(cursor)
            
            elif args.summaries:
                logger.info("Generating Summaries report for CLI...")
                # Call the internal function
                output_content = _get_all_summaries_markdown(cursor)
                
            elif args.graphs:
                logger.info("Generating Call Graphs report for CLI...")
                # Call the internal function
                output_content = _get_all_call_flows_mermaid(cursor)
        
        # Write the output to file
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(output_content)
            
        logger.info(f"Successfully wrote report to {output_path}")

    except Exception as e:
        logger.critical(f"An error occurred during CLI export: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    setup_logging()
    main()