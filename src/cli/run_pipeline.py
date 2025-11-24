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

def main():
    parser = argparse.ArgumentParser(description="Run the Mainframe Analysis Pipeline")
    
    step_names = [s["name"] for s in PIPELINE_STEPS]
    
    # Create a mutually exclusive group so user can't use both flags
    group = parser.add_mutually_exclusive_group()
    
    group.add_argument(
        "--start-from",
        type=str,
        choices=step_names,
        default="setup", # Default behavior is to run full pipeline
        help="The step to start processing from. Runs this step and all subsequent steps."
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
        help="List all available pipeline steps and exit."
    )

    args = parser.parse_args()
    setup_logging()

    # Handle --list-steps
    if args.list_steps:
        print("\nAvailable Pipeline Steps:")
        for i, step in enumerate(PIPELINE_STEPS):
            print(f"  {i+1}. {step['name']:<10} - {step['description']}")
        sys.exit(0)

    # Execution Loop
    total_start_time = time.time()
    
    # 1. Determine execution strategy
    if args.only:
        # Run ONLY one step
        target_step_name = args.only
        logger.info(f"Pipeline initialized. Running ONLY step: '{target_step_name}'")
        
        # Find the single step dict
        steps_to_run = [s for s in PIPELINE_STEPS if s["name"] == target_step_name]
        
    else:
        # Run SEQUENCE starting from start_from
        start_step_name = args.start_from
        logger.info(f"Pipeline initialized. Starting SEQUENCE from step: '{start_step_name}'")
        
        try:
            start_index = step_names.index(start_step_name)
            steps_to_run = PIPELINE_STEPS[start_index:]
        except ValueError:
            logger.critical(f"Invalid start step: {start_step_name}")
            sys.exit(1)

    # 2. Execute selected steps
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