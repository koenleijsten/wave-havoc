import os
import shutil
import unittest
from unittest.mock import patch, MagicMock, mock_open
from src.wave_havoc.get_data import (
    remove_existing_data,
    get_data,
    extract_data,
    clean_up_data_archive,
)


class TestFunctions(unittest.TestCase):
    @patch("shutil.rmtree")
    def test_remove_existing_data(self, mock_rmtree):
        output_dir = "/path/to/output"
        mock_rmtree.return_value = None

        os.path.exists = MagicMock(return_value=True)
        remove_existing_data(output_dir)
        mock_rmtree.assert_called_once_with(output_dir)

    @patch("shutil.rmtree")
    def test_remove_non_existing_data(self, mock_rmtree):
        output_dir = "/path/to/output"
        mock_rmtree.return_value = None

        os.path.exists = MagicMock(return_value=False)
        remove_existing_data(output_dir)
        mock_rmtree.assert_not_called()

    @patch("requests.get")
    @patch("builtins.open", new_callable=mock_open)
    def test_get_data_success(self, mock_open, mock_get):
        url = "http://example.com/data.tgz"
        output_dir = "/path/to/output"
        mock_response = MagicMock()
        mock_response.content = b"dummy content"
        mock_get.return_value = mock_response

        get_data(url, output_dir)
        mock_get.assert_called_once_with(url)
        mock_open.assert_called_once_with(os.path.join(output_dir, "data.tgz"), "wb")
        handle = mock_open()
        handle.write.assert_called_once_with(b"dummy content")

    @patch("requests.get")
    @patch("builtins.open", new_callable=mock_open)
    def test_get_data_failure(self, mock_open, mock_get):
        url = "http://example.com/data.tgz"
        output_dir = "/path/to/output"
        handle = mock_open()
        mock_open.return_value = handle
        mock_get.side_effect = Exception("Some error")

        with self.assertRaises(Exception) as context:
            get_data(url, output_dir)

        mock_get.assert_called_with(url)
        handle.write.assert_not_called()

    @patch("tarfile.open")
    @patch("builtins.print")
    def test_extract_data_success(self, mock_print, mock_tar_open):
        mock_tarfile = mock_tar_open.return_value.__enter__.return_value
        mock_tarfile.extractall.side_effect = lambda path, filter=None: None

        extract_data("output_dir")
        mock_tar_open.assert_called_once_with("output_dir/data.tgz", "r:gz")
        mock_tarfile.extractall.assert_called_once_with("output_dir", filter=None)
        mock_print.assert_not_called()

    @patch("tarfile.open")
    @patch("builtins.print")
    def test_extract_data_failure(self, mock_print, mock_tar_open):
        mock_tar_open.side_effect = FileNotFoundError("File not found")

        with self.assertRaises(FileNotFoundError) as context:
            extract_data("output_dir")

        mock_print.assert_not_called()

    @patch("os.path.exists", return_value=True)
    @patch("os.remove")
    def test_clean_up_data_archive_success(self, mock_remove, mock_exists):
        output_dir = "/path/to/output"

        clean_up_data_archive(output_dir)
        mock_exists.assert_called_once_with(os.path.join(output_dir, "data.tgz"))
        mock_remove.assert_called_once_with(os.path.join(output_dir, "data.tgz"))

    @patch("os.path.exists", return_value=False)
    @patch("os.remove")
    def test_clean_up_data_archive_failure(self, mock_remove, mock_exists):
        output_dir = "/path/to/output"

        clean_up_data_archive(output_dir)
        mock_exists.assert_called_once_with(os.path.join(output_dir, "data.tgz"))
        mock_remove.assert_not_called()


if __name__ == "__main__":
    unittest.main()
