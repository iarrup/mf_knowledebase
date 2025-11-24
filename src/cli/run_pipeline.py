import argparse
import logging
import sys
import time
from typing import List, Dict, Any

from src.core.logger import setup_logging

# Import the pipeline entry points
from src.pipeline.db_setup import run_db_setup
from src.pipeline.ingest import run_ingest
from src.pipeline.process import run_process
from src.pipeline.generate_stories import run_generate_stories

logger = logging.getLogger(__name__)

# Define the pipeline steps in order
PIPELINE_STEPS: List[Dict[str, Any]] = [
    {
        "name": "setup",
        "func": run_db_setup,
        "description": "Database Schema Setup (Destructive: Drops existing tables)"
    },
    {
        "name": "ingest",
        "func": run_ingest,
        "description": "Raw Artifact Ingestion (Reads files from Source)"
    },
    {
        "name": "process",
        "func": run_process,
        "description": "Processing (Summarization, Graph Building, Vectorization)"
    },
    {
        "name": "stories",
        "func": run_generate_stories,
        "description": "User Story Generation"
    }
]

def print_banner(step_name: str, description: str):
    """Prints a visual separator for logs."""
    border = "=" * 60
    logger.info(f"\n{border}\n🚀 STARTING STEP: {step_name.upper()}\nℹ️  {description}\n{border}")

def print_steps():
    """Helper to print available steps to stdout."""
    print("\nAvailable Pipeline Steps:")
    for i, step in enumerate(PIPELINE_STEPS):
        print(f"  {i+1}. {step['name']:<10} - {step['description']}")
    print("")

def main():
    # Construct a detailed help string for the --help output
    step_details = "\n".join([f"  * {s['name']}: {s['description']}" for s in PIPELINE_STEPS])
    
    parser = argparse.ArgumentParser(
        description="Run the Mainframe Analysis Pipeline.\n\nAvailable Steps:\n" + step_details,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    step_names = [s["name"] for s in PIPELINE_STEPS]
    
    # Mutually exclusive group: user must choose ONE mode of operation
    group = parser.add_mutually_exclusive_group()
    
    group.add_argument(
        "--all",
        action="store_true",
        help="Run the ENTIRE pipeline sequentially (setup -> ingest -> process -> stories)."
    )
    
    group.add_argument(
        "--start-from",
        type=str,
        choices=step_names,
        help="Start from a specific step and run all subsequent steps."
    )
    
    group.add_argument(
        "--only",
        type=str,
        choices=step_names,
        help="Run ONLY this specific step and exit."
    )
    
    parser.add_argument(
        "--list-steps",
        action="store_true",
        help="List available steps and exit."
    )

    # 1. Handle "No Arguments" case
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()
    setup_logging()

    # 2. Handle List Steps
    if args.list_steps:
        print_steps()
        sys.exit(0)

    # 3. Determine Execution Strategy
    steps_to_run = []

    if args.all:
        logger.info("Pipeline initialized. Mode: FULL SEQUENCE (--all)")
        steps_to_run = PIPELINE_STEPS

    elif args.only:
        target_step_name = args.only
        logger.info(f"Pipeline initialized. Mode: SINGLE STEP (--only {target_step_name})")
        steps_to_run = [s for s in PIPELINE_STEPS if s["name"] == target_step_name]
        
    elif args.start_from:
        start_step_name = args.start_from
        logger.info(f"Pipeline initialized. Mode: RESUME SEQUENCE (--start-from {start_step_name})")
        start_index = step_names.index(start_step_name)
        steps_to_run = PIPELINE_STEPS[start_index:]

    # 4. Execute Logic
    if not steps_to_run:
        # Should be caught by the sys.argv check, but safety fallback
        print("No action selected.")
        print_steps()
        sys.exit(0)

    total_start_time = time.time()

    for step in steps_to_run:
        step_start_time = time.time()
        print_banner(step['name'], step['description'])
        
        try:
            # Execute the pipeline function
            step['func']()
            
            elapsed = time.time() - step_start_time
            logger.info(f"✅ Finished step '{step['name']}' in {elapsed:.2f} seconds.")
            
        except Exception as e:
            logger.critical(f"❌ Pipeline failed at step '{step['name']}': {e}", exc_info=True)
            sys.exit(1)

    total_elapsed = time.time() - total_start_time
    logger.info(f"\n🎉 Execution completed successfully in {total_elapsed:.2f} seconds.")

if __name__ == "__main__":
    main()