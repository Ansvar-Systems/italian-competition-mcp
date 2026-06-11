# Tools Reference

All tools provided by the AGCM Competition MCP server. Tool prefix: `it_comp_`.

## it_comp_search_decisions

Full-text search across AGCM enforcement decisions (abuse of dominance, cartel, sector inquiries).

**Parameters**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | Yes | Search query (e.g., `'intesa anticoncorrenziale'`, `'abuso di posizione dominante'`) |
| `type` | enum | No | Filter by decision type: `abuse_of_dominance`, `cartel`, `merger`, `sector_inquiry` |
| `sector` | string | No | Filter by sector ID (e.g., `'digitale'`, `'energia'`) |
| `outcome` | enum | No | Filter by outcome: `prohibited`, `cleared`, `cleared_with_conditions`, `fine` |
| `limit` | number | No | Maximum results to return (default: 20, max: 100) |

**Returns** Array of matching decisions with `_citation` per item and `_meta` block.

---

## it_comp_get_decision

Get a specific AGCM enforcement decision by case number.

**Parameters**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `case_number` | string | Yes | AGCM case number (e.g., `'A555'`, `'I839'`, `'C12345'`) |

**Returns** Full decision record with `_citation` and `_meta` block, or `not_found` error.

---

## it_comp_search_mergers

Search AGCM merger control decisions.

**Parameters**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | Yes | Search query (e.g., `'Mediaset'`, `'Enel'`) |
| `sector` | string | No | Filter by sector ID |
| `outcome` | enum | No | Filter by outcome: `cleared`, `cleared_phase1`, `cleared_with_conditions`, `prohibited` |
| `limit` | number | No | Maximum results to return (default: 20, max: 100) |

**Returns** Array of matching merger cases with `_citation` per item and `_meta` block.

---

## it_comp_get_merger

Get a specific AGCM merger control decision by case number.

**Parameters**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `case_number` | string | Yes | AGCM merger case number (e.g., `'C12345'`) |

**Returns** Full merger record with `_citation` and `_meta` block, or `not_found` error.

---

## it_comp_list_sectors

List all sectors with AGCM enforcement activity, including decision and merger counts per sector.

**Parameters** None.

**Returns** Array of sector objects with `id`, `name`, `name_en`, `description`, `decision_count`, `merger_count`, and `_meta` block.

---

## it_comp_about

Return metadata about this MCP server.

**Parameters** None.

**Returns** Server name, version, description, data source, coverage summary, and tool list.

---

## it_comp_list_sources

List the data sources used by this MCP server.

**Parameters** None.

**Returns** Array of source objects with `name`, `url`, and `description`.

---

## it_comp_check_data_freshness

Check when the AGCM data was last updated and whether it is current or stale.

**Parameters** None.

**Returns** Object with `last_updated` (ISO date), `source`, `status` (`current` | `stale`), and `age_days`.

---

## Response Envelope

All successful responses include a `_meta` block:

```json
{
  "_meta": {
    "disclaimer": "This information is for research purposes only...",
    "data_age": "YYYY-MM-DD",
    "copyright": "Autorita Garante della Concorrenza e del Mercato (AGCM)",
    "source_url": "https://www.agcm.it/"
  }
}
```

Error responses include `_error_type` (`not_found` | `internal_error`) alongside `_meta`.

Search result items include a `_citation` block for entity linking:

```json
{
  "_citation": {
    "canonical_ref": "A555",
    "display_text": "Decision A555",
    "lookup": {
      "tool": "it_comp_get_decision",
      "args": { "case_number": "A555" }
    }
  }
}
```
