from pathlib import Path
from tempfile import TemporaryDirectory
from contextlib import contextmanager

from opendal import Operator


class OpenDALService:
    def copy(
        self,
        source_client,
        source_path,
        destination_client,
        destination_path,
        read_chunk_size=1024,
    ):
        with source_client.open(source_path, "rb") as source_file:
            with destination_client.open(destination_path, "wb") as destination_file:
                bytes_to_write = source_client.stat(source_path).content_length
                while bytes_to_write > 0:
                    if bytes_to_write < read_chunk_size:
                        chunk_size_to_read = bytes_to_write
                    else:
                        chunk_size_to_read = read_chunk_size
                    chunk = bytes(source_file.read(chunk_size_to_read))
                    destination_file.write(chunk)
                    bytes_to_write -= chunk_size_to_read

    def download(self, download_client, download_from_path, save_path):
        source_client = download_client
        source_path = download_from_path
        destination_client = Operator("fs", root="/")
        destination_path = str(save_path)
        self.copy(source_client, source_path, destination_client, destination_path)

    @contextmanager
    def download_to_temporary_file(self, download_client, download_from_path, file_name=None):
        download_from_path = str(download_from_path)
        try:
            temp_directory = TemporaryDirectory()
            if file_name is None:
                file_name = download_from_path.split("/")[-1]
            save_path = Path(temp_directory.name) / file_name
            self.download(download_client, download_from_path, str(save_path))
            yield save_path
        finally:
            temp_directory.cleanup()
