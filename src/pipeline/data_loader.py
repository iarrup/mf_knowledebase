import logging
import subprocess
import sys
from pathlib import Path
import shutil

from src.core.config import settings

logger = logging.getLogger(__name__)
# This is where the cloned repo will be stored
CLONE_DIR = Path("data/external/github_clone")

def _run_git_command(command: list, cwd: Path = None):
    """Runs a Git command, logs output, and raises an error on failure."""
    logger.info(f"Running git command: {' '.join(command)}")
    try:
        # We use check=True to automatically raise an error if git fails
        result = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True,
            encoding='utf-8'
        )
        logger.debug(f"Git stdout: {result.stdout}")
        if result.stderr:
            logger.info(f"Git stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logger.critical(f"Git command failed with exit code {e.returncode}")
        logger.critical(f"stdout: {e.stdout}")
        logger.critical(f"stderr: {e.stderr}")
        raise
    except FileNotFoundError:
        logger.critical("Git command not found. Please ensure 'git' is installed and in your system's PATH.")
        raise

def _load_from_github() -> Path:
    """Clones or pulls a GitHub repo and returns the target path."""
    url = settings.GITHUB_REPO_URL
    token = settings.GITHUB_ACCESS_TOKEN
    sub_path = settings.GITHUB_REPO_PATH

    if not url or not sub_path:
        logger.critical("DATA_SOURCE_TYPE is 'github' but GITHUB_REPO_URL or GITHUB_REPO_PATH is not set.")
        raise ValueError("Missing GitHub configuration in environment.")

    # Ensure the parent directory exists
    CLONE_DIR.parent.mkdir(parents=True, exist_ok=True)

    # Check if repo is already cloned
    if CLONE_DIR.exists() and (CLONE_DIR / ".git").exists():
        logger.info(f"Repo already exists at {CLONE_DIR}. Pulling latest changes...")
        try:
            _run_git_command(["git", "pull"], cwd=CLONE_DIR)
        except Exception as e:
            logger.warning(f"Failed to pull latest changes: {e}. Using existing files.")
    else:
        logger.info(f"Cloning repo from {url} to {CLONE_DIR}...")
        if CLONE_DIR.exists():
            logger.warning(f"Removing incomplete clone directory: {CLONE_DIR}")
            shutil.rmtree(CLONE_DIR)
        
        # Format URL with token for private repos
        if token:
            url = url.replace("https://", f"https://{token}@")
        
        # Clone with --depth 1 (we only need the latest files)
        _run_git_command(["git", "clone", "--depth", "1", url, str(CLONE_DIR)])

    target_path = CLONE_DIR / sub_path
    if not target_path.exists() or not target_path.is_dir():
        logger.critical(f"The specified GITHUB_REPO_PATH '{sub_path}' does not exist in the cloned repo.")
        raise FileNotFoundError(f"Path not found in repo: {target_path}")

    logger.info(f"GitHub source loaded successfully from: {target_path}")
    return target_path

def load_data_source() -> Path:
    """
    Main function to load COBOL files from the configured source.
    Returns a Path object to the directory containing the files.
    """
    source_type = settings.DATA_SOURCE_TYPE.lower()
    
    if source_type == "local":
        logger.info("Loading from LOCAL data source.")
        path = Path("data/input/prog")
        if not path.exists():
            logger.critical(f"Local data directory not found: {path}")
            raise FileNotFoundError(f"Local data directory not found: {path}")
        return path
    
    elif source_type == "github":
        logger.info("Loading from GITHUB data source.")
        return _load_from_github()
    
    else:
        raise ValueError(f"Invalid DATA_SOURCE_TYPE: '{source_type}'. Must be 'local' or 'github'.")