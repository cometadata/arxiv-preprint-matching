# Get Sample from Query

Queries the DataCite API to collect a random sample of DOIs for a given query, with optional grouping by client, provider, or resource type.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python get_sample_from_query.py -q QUERY -s SAMPLE_SIZE 
                               [-g {client,provider,resource-type}] [-n NUM_GROUPS]
                               [-d DELAY] [-o OUTPUT_DIR]
```

### Required Arguments
- `-q, --query`: Query string (e.g., "client-id=datacite.datacite" or "query=climate")
- `-s, --sample_size`: Number of records per group (or total if no grouping)

### Optional Arguments
- `-g, --group_by`: Group results by client, provider, or resource-type
- `-n, --num_groups`: Number of groups to sample (default: 20)
- `-d, --delay`: Delay between API requests in seconds (default: 1.0)
- `-o, --output_dir`: Output directory (default: timestamp_normalized_query)

## Examples

Sample 1000 random DOIs where the publisher is arXiv, which have a related identifer whose type is `IsVersionOf` and  a DOI as related identifier value:
```bash
python get_sample_from_query.py -q "publisher.name:arXiv AND relatedIdentifiers.relationType:IsVersionOf AND relatedIdentifiers.relatedIdentifierType:DOI" -s 1000
```

## Output Structure

```
output_dir/
├── csv/
│   └── timestamp_query.csv
└── json/
    └── DOI_PREFIX/
        └── DOI.json
```