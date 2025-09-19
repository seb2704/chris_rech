import re

import pdfplumber
import polars as pl

pattern_umsatzsteuer = re.compile(r"Umsatzsteuer.*?(\d+(?:[.,]\d+)?)\s*%")
pattern_rechnungsdatum = re.compile(r"Rechnungsdatum\s*(\d{2}\.\d{2}\.\d{4})")



def extract_rechnungen(file):
    umsatzsteuer = 0
    with pdfplumber.open(file) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()
        print(text)
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
    headers = table[0]
    rows = table[1:]
    umsatzszeuer_string = str(umsatzsteuer) + "(%)"
    df = pl.DataFrame(rows, schema=headers)
    df = df.with_columns([
        pl.lit(umsatzszeuer_string).alias("Ust(%)"),
        pl.lit(name).alias("Name"),
        pl.lit(date).alias("Rechnungsdatum"),
        (pl.col("Preis").str.replace("€", "")  # € entfernen
         .str.strip_chars()
         .str.replace(",", ".")  # führende/abschließende Leerzeichen entfernen
         .cast(pl.Float64) * float(str(umsatzsteuer).replace(",", ".")) / 100).alias("Ust")
    ])
    return df.drop("Po")
files = ["test.pdf","test2.pdf"]
df_all = None
for file in files:
    df = extract_rechnungen(file)
    if df_all is None:
        df_all = df
    else:
        df_all = pl.concat([df_all, df], how="vertical")
print(df_all)
