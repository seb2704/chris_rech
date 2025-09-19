import re

import pdfplumber
from pathlib import Path
import polars as pl

pattern_umsatzsteuer = re.compile(r"Umsatzsteuer.*?(\d+(?:[.,]\d+)?)\s*%")
pattern_rechnungsdatum = re.compile(r"Rechnungsdatum\s*(\d{2}\.\d{2}\.\d{4})")

pattern_rechnungsnummer = re.compile(r"Rechnung \s*(RE-PS-\d{4}-\d+)")



def extract_rechnungen(file):
    try:
        umsatzsteuer = 0
        with pdfplumber.open(file) as pdf:
            page = pdf.pages[0]
            text = page.extract_text()
            table = page.extract_table()

            lines = text.splitlines()
            for i, line in enumerate(lines):

                match = pattern_umsatzsteuer.search(line)
                if match:
                    value = match.group(1)
                    umsatzsteuer = value
                if line.strip().startswith("Rechnung Rechnungsadresse:"):
                    if i + 1 < len(lines):
                        next_line = lines[i + 1].strip()
                        name = next_line
                match = pattern_rechnungsdatum.search(line)
                if match:
                    # nur den Teil vor dem Datum behalten
                    date = match.group(1)  # "Rechn
                match = pattern_rechnungsnummer.search(line)
                if match:
                    # nur den Teil vor dem Datum behalten
                    rechnung = match.group(1)  # "Rechn
                
        headers = table[0]
        rows = table[1:]
        umsatzszeuer_string = str(umsatzsteuer) + "(%)"
        df = pl.DataFrame(rows, schema=headers)
        df = df.with_columns([
            pl.lit(umsatzszeuer_string).alias("Ust(%)"),
            pl.lit(rechnung).alias("Rechnungsnummer"),
            pl.lit(name).alias("Name"),
            pl.lit(date).alias("Rechnungsdatum"),
            (pl.col("Preis").str.replace("€", "")  # € entfernen
             .str.strip_chars()
             .str.replace(",", ".")  # führende/abschließende Leerzeichen entfernen
             .cast(pl.Float64) * float(str(umsatzsteuer).replace(",", ".")) / 100).alias("Ust")
        ])
    except Exception as e:
        logger.error(f"Fehler beim Verarbeiten der Datei {file}: {e}")
        return None
    return df
files = list(Path(".").glob("*.pdf"))
df_all = None
for file in files:
    print(file)
    df = extract_rechnungen(file)
    if df is not None:
        if df_all is None:
            df_all = df
        else:
            df_all = pl.concat([df_all, df], how="vertical")
df_all = df_all.select(["Rechnungsdatum","Artikel", "Rechnungsnummer","Name","Anzahl","Preis","Summe","Ust(%)"])
df_all.write_excel("rechnungen.xlsx")
