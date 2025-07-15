# MIFIR Orderbook Data Processing Utils

Project provides utilities for processing, analyzing, and exporting orderbook data in compliance with MiFIR (Markets in Financial Instruments Regulation) reporting requirements. [MiFIR Regulatory Technical Standards](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=uriserv:OJ.L_.2017.087.01.0193.01.ENG&toc=OJ:L:2017:087:TOC)

## Features

- **Orderbook Data Extraction:** Reads and filters orderbook event data from CSV files ([`orderbook/db/csvreader.py`](src/orderbook/db/csvreader.py)).
- **Statistics Calculation:** Computes daily trading statistics per ticker ([`orderbook/etl/orderbookstats.py`](src/orderbook/etl/orderbookstats.py)).
- **ETL Pipeline:** Pipeline for extracting, transforming, and loading orderbook statistics into a sqlite database ([`orderbook/etl/pipeline.py`](src/orderbook/etl/pipeline.py), [`orderbook/db/statssqlite.py`](src/orderbook/db/statssqlite.py)).
- **XML Generation:** Converts orderbook data into ESMA-compliant XML files for regulatory reporting ([`orderbook/xmlgen/converter.py`](src/orderbook/xmlgen/converter.py)).
- **Schema Validation:** Validates generated XML files against the official XSD schema ([`schemas/auth.anonym.113.001.01.xsd`](src/schemas/auth.anonym.113.001.01.xsd)).

## Project Structure

- `src/orderbook/`
  - `db/` - Database access and CSV reading
  - `etl/` - ETL pipeline and statistics computation
  - `xmlgen/` - XML generation and schema validation

## Usage

1. **Extract and Process Data:**
   - Configure the data source and run the ETL pipeline to process new files and update statistics.
   - Example: 

```python
from orderbook.etl import pipeline

market = "INET_MainMarket"

pipeline.process_date(market, datetime(2025, 3, 17))
```

2. **Generate XML Reports:**
   - Use the XML generator to create ESMA-compliant XML files from processed data.
   - Files are validated and zipped
   - Example:

```python
from orderbook.xmlgen import adhoc_pipe

start = datetime(2023, 1, 1)
end = datetime(2023, 6, 30)

adhoc_pipe.convert_period(
    start=start,
    end=end,
    path="output_folder/",
)
```

## License

All rights retained.