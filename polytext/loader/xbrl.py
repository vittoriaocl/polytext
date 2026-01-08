import os
import tempfile
import logging
import xml.etree.ElementTree as ET

from ..exceptions import EmptyDocument
from ..loader.downloader.downloader import Downloader

logger = logging.getLogger(__name__)


class XBRLLoader:
    """
    Loader per file XBRL:
    - supporta local, S3, GCS
    - parsing XML puro
    """

    def __init__(
        self,
        source="local",
        s3_client=None,
        document_aws_bucket=None,
        gcs_client=None,
        document_gcs_bucket=None,
        temp_dir="temp",
        markdown_output=True,
        **kwargs,
    ):
        self.source = source
        self.s3_client = s3_client
        self.document_aws_bucket = document_aws_bucket
        self.gcs_client = gcs_client
        self.document_gcs_bucket = document_gcs_bucket
        self.markdown_output = markdown_output
        self.type = "xbrl"

        self.temp_dir = os.path.abspath(temp_dir)
        os.makedirs(self.temp_dir, exist_ok=True)
        tempfile.tempdir = self.temp_dir

    def load(self, input_path: str) -> dict:
        logger.info(f"Loading XBRL file: {input_path}")

        if self.source == "cloud":
            file_path = self._download_to_temp(input_path)
        else:
            file_path = input_path

        try:
            text = self._extract_xbrl_text(file_path)
        finally:
            if self.source == "cloud" and os.path.exists(file_path):
                os.remove(file_path)

        if not text.strip():
            raise EmptyDocument("XBRL file contains no readable textual content")

        return {
            "text": text,
            "completion_tokens": 0,
            "prompt_tokens": 0,
            "completion_model": "not provided",
            "completion_model_provider": "not provided",
            "text_chunks": "not provided",
            "type": self.type,
            "input": input_path,
        }

    def _download_to_temp(self, file_path: str) -> str:
        fd, temp_path = tempfile.mkstemp(suffix=".xbrl")
        os.close(fd)

        downloader = Downloader(
            s3_client=self.s3_client,
            document_aws_bucket=self.document_aws_bucket,
            gcs_client=self.gcs_client,
            document_gcs_bucket=self.document_gcs_bucket,
        )

        if self.s3_client:
            downloader.download_file_from_s3(file_path, temp_path)
        elif self.gcs_client:
            downloader.download_file_from_gcs(file_path, temp_path)
        else:
            raise ValueError("Cloud source specified but no cloud client provided")

        return temp_path

    def _extract_xbrl_text(self, file_path: str) -> str:
        tree = ET.parse(file_path)
        root = tree.getroot()

        lines = []
        for elem in root.iter():
            if elem.text:
                value = elem.text.strip()
                if not value:
                    continue
                tag = elem.tag.split("}")[-1]
                lines.append(f"{tag}: {value}")

        return "\n".join(lines)



# # Standard library imports
# import os
# import tempfile
# import logging
# import xml.etree.ElementTree as ET
#
# # Local imports
# from ..exceptions import EmptyDocument
#
# logger = logging.getLogger(__name__)
#
#
# class XBRLLoader:
#     """
#     Loader per file XBRL locali.
#     - Supporta solo file locali (.xbrl / .xml)
#     - Estrae i contenuti testuali dagli elementi XML
#     - Restituisce un output compatibile con BaseLoader
#     """
#
#     def __init__(
#         self,
#         markdown_output: bool = True,
#         temp_dir: str = "temp",
#         **kwargs,
#     ):
#         self.markdown_output = markdown_output
#         self.type = "xbrl"
#
#         # Setup temp dir (coerente con gli altri loader)
#         self.temp_dir = os.path.abspath(temp_dir)
#         os.makedirs(self.temp_dir, exist_ok=True)
#         tempfile.tempdir = self.temp_dir
#
#     def load(self, input_path: str) -> dict:
#         """
#         Carica ed estrae testo da un file XBRL locale.
#         Args:
#             input_path (str): percorso locale al file .xbrl
#         Returns:
#             dict: output standard Polytext
#         """
#
#         logger.info(f"Loading XBRL file: {input_path}")
#
#         if not os.path.exists(input_path):
#             raise FileNotFoundError(f"XBRL file not found: {input_path}")
#
#         text = self._extract_xbrl_text(input_path)
#
#         if not text.strip():
#             raise EmptyDocument("XBRL file contains no readable textual content")
#
#         return {
#             "text": text,
#             "completion_tokens": 0,
#             "prompt_tokens": 0,
#             "completion_model": "not provided",
#             "completion_model_provider": "not provided",
#             "text_chunks": "not provided",
#             "type": self.type,
#             "input": input_path,
#         }
#
#     def _extract_xbrl_text(self, file_path: str) -> str:
#         """
#         Estrae contenuto testuale leggibile da XBRL (XML).
#
#         Strategia:
#         - rimuove namespace
#         - legge tutti i nodi con testo
#         - restituisce coppie chiave: valore
#         """
#
#         tree = ET.parse(file_path)
#         root = tree.getroot()
#
#         extracted_lines = []
#
#         for elem in root.iter():
#             if elem.text:
#                 value = elem.text.strip()
#                 if not value:
#                     continue
#
#                 # Rimuove namespace XML (es: {http://...}Assets → Assets)
#                 tag = elem.tag.split("}")[-1]
#
#                 extracted_lines.append(f"{tag}: {value}")
#
#         return "\n".join(extracted_lines)
