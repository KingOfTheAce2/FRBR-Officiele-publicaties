# Dutch Officiële Publicaties SRU Crawler

This repo contains a crawler that fetches XML records from the official SRU 2.0 endpoint at `repository.overheid.nl` for the `officiële publicaties` collection.

## Features

- SRU 2.0 querying with pagination
- Resumes from last fetched record (via `sru_state.json`)
- Cleans and extracts text content
- Outputs newline-delimited JSON (`output.jsonl`)
- Uploads to Hugging Face dataset: `vGassen/Dutch-Officiele-Publicaties`

## Configuration

Set your Hugging Face token as an environment variable:

```bash
export HF_TOKEN=your_token_here
