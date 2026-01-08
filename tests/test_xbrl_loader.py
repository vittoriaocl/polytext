import os
import sys
import logging

# Aggiunge la root del progetto al PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv(".env")

from polytext.loader.base import BaseLoader

# Setup logging (consigliato per debug loader)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Percorso del file XBRL di test (WINDOWS)
XBRL_FILE_PATH = r"C:\Users\vocleppo\Downloads\31_12_2024_BilancioCEEeNotaIntegrativa_OPENDotComSpa.xbrl"


def main():
    loader = BaseLoader(
        markdown_output=True,
        source="local",   # FONDAMENTALE
    )

    result = loader.get_text(
        input_list=[XBRL_FILE_PATH]
    )

    # ---- OUTPUT DI TEST ----
    print("\n=== XBRL LOADER TEST ===\n")
    print("TYPE:", result["output_list"][0]["type"])
    print("INPUT:", result["output_list"][0]["input"])
    print("\n--- TEXT (first 2000 chars) ---\n")
    print(result["text"][:2000])

    # Salvataggio su file per ispezione manuale
    output_file = "xbrl_extracted_text.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(result["text"])

    print(f"\n✔ Output salvato in: {output_file}")


if __name__ == "__main__":
    main()
