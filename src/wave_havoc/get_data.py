import os
import shutil
import requests
import tarfile
from typing import Optional


def remove_existing_data(output_dir: str) -> None:
    """Remove the output directory if it exists.

    Args:
        output_dir (str): The path to the output directory.
    """
    try:
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
    except Exception as e:
        raise e


def get_data(url: str, output_dir: str) -> None:
    """Download data from the given URL and save it to the output directory.

    Args:
        url (str): The URL to download the data from.
        output_dir (str): The path to the output directory.
    """
    response = requests.get(url)
    response.raise_for_status()

    with open(os.path.join(output_dir, "data.tgz"), "wb") as f:
        f.write(response.content)


def extract_data(output_dir: str, filter: Optional[str] = None) -> None:
    """Extract data from the tar.gz file to the output directory.

    Args:
        output_dir (str): The path to the output directory.
        filter (str, optional): Filter to extract specific files or directories.
    """
    with tarfile.open(os.path.join(output_dir, "data.tgz"), "r:gz") as tar:
        tar.extractall(output_dir, filter=filter)


def clean_up_data_archive(output_dir: str) -> None:
    """Clean up the data archive file after extraction.

    Args:
        output_dir (str): The path to the output directory.
    """

    archive_file = os.path.join(output_dir, "data.tgz")
    if os.path.exists(archive_file):
        os.remove(archive_file)
