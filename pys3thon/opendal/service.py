from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from opendal import Operator

DEFAULT_CHUNK_SIZE_256MB = 256 * 1024 * 1024


class OpenDALService:
    def copy(
        self,
        source_client,
        source_path,
        destination_client,
        destination_path,
        read_chunk_size=DEFAULT_CHUNK_SIZE_256MB,
    ):
        total_size = source_client.stat(source_path).content_length
        bytes_written = 0

        with source_client.open(source_path, "rb") as source_file:
            with destination_client.open(destination_path, "wb") as destination_file:
                while bytes_written < total_size:
                    # Calculate remaining bytes
                    remaining = total_size - bytes_written
                    chunk_size = min(read_chunk_size, remaining)

                    # Read and write chunk directly without unnecessary conversion
                    chunk = source_file.read(chunk_size)
                    if not chunk:  # EOF
                        break

                    destination_file.write(chunk)
                    bytes_written += len(chunk)

        # Verify the copy was complete
        if bytes_written != total_size:
            raise IOError(
                f"Copy incomplete. Expected {total_size} bytes but wrote {bytes_written} bytes"
            )

    def download(self, download_client, download_from_path, save_path):
        source_client = download_client
        source_path = download_from_path
        destination_client = Operator("fs", root="/")
        destination_path = str(save_path)
        self.copy(source_client, source_path, destination_client, destination_path)

    @contextmanager
    def download_to_temporary_file(
        self, download_client, download_from_path, file_name=None
    ):
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
