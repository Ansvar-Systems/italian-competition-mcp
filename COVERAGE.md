# Coverage

This document describes the corpus covered by the AGCM Competition MCP server.

## Authority

**Autorita Garante della Concorrenza e del Mercato (AGCM)**
Italian Competition Authority
- Website: https://www.agcm.it/
- Legal basis: Legge n. 287/1990 — Norme per la tutela della concorrenza e del mercato

## Corpus Scope

### Enforcement Decisions (`decisions` table)

| Attribute | Detail |
|-----------|--------|
| **Case types** | Abuse of dominance (A-prefix), cartel/antitrust (I-prefix), sector inquiries (IC-prefix), consumer protection (PS-prefix) |
| **Legal basis** | Articles 2, 3, and 6 of Legge 287/1990; EU Articles 101 and 102 TFEU for cases with EU dimension |
| **Date range** | 1990–present (subject to ingestion completeness) |
| **Completeness** | Partial — see `data/ingest-state.json` for current ingestion counts |
| **Language** | Italian (original); some summaries available in English |

### Merger Control Decisions (`mergers` table)

| Attribute | Detail |
|-----------|--------|
| **Case types** | Phase I clearances, Phase II investigations, conditional clearances, prohibitions |
| **Case prefix** | C-prefix (e.g., C12345) |
| **Legal basis** | Articles 16–19 of Legge 287/1990 (turnover thresholds for Italian notification) |
| **Date range** | 1990–present (subject to ingestion completeness) |
| **Completeness** | Partial — see `data/ingest-state.json` for current ingestion counts |

### Sectors (`sectors` table)

Pre-populated sectors with enforcement activity:

- `digitale` — Digital economy / platforms
- `energia` — Energy (electricity, gas)
- `grande_distribuzione` — Large-scale retail / food distribution
- `servizi_finanziari` — Financial services / banking / insurance
- `farmaceutico` — Pharmaceutical / healthcare
- `telecomunicazioni` — Telecommunications
- `media` — Media / broadcasting
- `automotive` — Automotive

## Out of Scope

- Regional Italian competition enforcement (handled by courts, not AGCM)
- EU-level cases handled exclusively by the European Commission (DG COMP)
- Consumer protection cases that do not involve competition law
- Sector regulation by other Italian authorities (AGCOM, ARERA, IVASS)

## Data Currency

Data is ingested from AGCM's public bulletin via `scripts/ingest-agcm.ts`.
Last ingestion date is tracked in `data/ingest-state.json`.

Run `npm run ingest` to refresh data from AGCM's website.
Run `npm run seed` to populate the database with sample data for development.

## Machine-Readable Metadata

See `data/coverage.json` for a machine-readable summary of coverage metadata.
