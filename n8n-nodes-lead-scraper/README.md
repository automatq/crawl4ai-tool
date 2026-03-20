# n8n-nodes-lead-scraper

Custom n8n community node for the Lead Scraper tool. Scrape business websites for emails, phones, addresses, and more — directly from your n8n workflows.

## Prerequisites

Your Lead Scraper instance must be running (`python web.py`) and accessible from wherever n8n is hosted.

## Installation

### In n8n Desktop / Self-hosted

```bash
cd ~/.n8n
npm install /path/to/n8n-nodes-lead-scraper
```

Then restart n8n.

### Development

```bash
cd n8n-nodes-lead-scraper
npm install
npm run build
npm link

# In your n8n directory:
npm link n8n-nodes-lead-scraper
```

## Setup

1. Add a **Lead Scraper API** credential in n8n
2. Set the **Base URL** to your Lead Scraper instance (default: `http://localhost:5000`)
3. Drag the **Lead Scraper** node into your workflow

## Operations

### Search & Scrape
Search for businesses by keyword + cities, then scrape their websites.

- **Keyword**: Business type (e.g. "hvac", "dentist")
- **Cities**: Comma-separated (e.g. "Denver, Phoenix")
- **Max Leads**: How many results to collect

### Scrape URL
Scrape a specific URL for contact data.

### Options
- **Stealth Mode**: Randomize browser fingerprint
- **Google Maps**: Also search Google Maps
- **Deep Crawl**: Follow internal links
- **Concurrency**: Parallel scrapers (1-10)
- **Proxies**: Newline-separated proxy list
- **Timeout**: Max wait time (seconds)

## Output

Each lead is output as a separate item with:
- `url`, `company`, `description`
- `emails` (array), `phones` (array)
- `address`, `hours`
- `socials` (object with platform URLs)
