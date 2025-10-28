# Scraper Standardization Guidelines

## Required Data Elements

All scrapers must extract the following standardized elements:

### 1. Basic Concert Information
- **Title**: Concert title/name
- **Date**: Concert date and time (datetime object)
- **Venue**: Venue name and city
- **External URL**: Link to concert details on venue website

### 2. Performers
- **Name**: Performer name
- **Role**: Type of performer (conductor, soloist, orchestra, choir, etc.)
- **Extract from**: Individual concert pages, not just listing pages

### 3. Program/Repertoire
- **Composer**: Composer name
- **Title**: Piece/work title
- **Extract from**: Individual concert pages, program sections

### 4. Data Quality Standards
- **No duplicates**: Check by title + date + venue
- **Complete data**: Prefer concerts with performers and program
- **Consistent format**: Standardize date/time parsing
- **Error handling**: Graceful failure, continue processing

### 5. Implementation Requirements
- **Progress tracking**: Update progress during scraping
- **Venue timestamp**: Update venue.last_scraped when successful
- **Limit testing**: Use max_concerts = 5 for testing
- **Logging**: Detailed debug output for troubleshooting

## Scraper Structure

```python
def scrape(self):
    # 1. Get main listing page
    # 2. Find concert elements
    # 3. For each concert:
    #    - Extract basic info (title, date, venue)
    #    - Visit individual concert page
    #    - Extract performers and program
    #    - Save to database
    # 4. Update venue.last_scraped
    # 5. Return success status
```

## Data Extraction Priority

1. **High Priority**: Title, Date, Venue, External URL
2. **Medium Priority**: Performers (conductor, soloists)
3. **Low Priority**: Program/repertoire details

## Error Handling

- Continue processing if individual concerts fail
- Log errors but don't stop entire scraping process
- Return True if any concerts were successfully saved
- Update venue timestamp only on successful scraping
