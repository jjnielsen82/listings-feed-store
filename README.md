# Listings Feed Store

Automated data pipeline for Arizona real estate listings (Phoenix & Tucson).

## How It Works

1. **Local scrapers** run on your machine, sync CSVs to this GitHub repo
2. **GitHub Actions** automatically processes the data when CSVs are updated
3. **Website/apps** fetch pre-computed JSON from GitHub via jsDelivr CDN

## Data Files

### Input (data/)

- `phoenix_listings.csv` - All Phoenix MLS listings
- `tucson_listings.csv` - All Tucson MLS listings

### Output (output/)

- `phoenix_listings.json` - Processed Phoenix listings (deduplicated by MLS#)
- `tucson_listings.json` - Processed Tucson listings (deduplicated by MLS#)
- `verified_agents.json` - Unique agents by email (for Community Photos verification)
- `photographers.json` - Camera/photographer analytics from EXIF data

## JSON URLs (for website)

Use jsDelivr CDN for fast, cached delivery:

```
https://cdn.jsdelivr.net/gh/jnielsen82/listings-feed-store@main/output/verified_agents.json
https://cdn.jsdelivr.net/gh/jnielsen82/listings-feed-store@main/output/phoenix_listings.json
https://cdn.jsdelivr.net/gh/jnielsen82/listings-feed-store@main/output/tucson_listings.json
https://cdn.jsdelivr.net/gh/jnielsen82/listings-feed-store@main/output/photographers.json
```

Or use raw GitHub (slower, but always fresh):

```
https://raw.githubusercontent.com/jnielsen82/listings-feed-store/main/output/verified_agents.json
```

## CSV Schema

| Column | Description |
|--------|-------------|
| timestamp | When the listing was scraped |
| mls_number | **UNIQUE KEY** - MLS listing number |
| price | Listing price |
| listing_address | Full property address |
| status | Active, Cancelled, Closed, Expired |
| agent_name | Listing agent full name |
| agent_first_name | Agent first name |
| agent_phone | Agent phone number |
| agent_email | **KEY FOR VERIFICATION** |
| agent_website | Agent website |
| office_name | Brokerage name |
| office_phone | Office phone |
| office_email | Office email |
| office_website | Office website |
| formatted_address | Standardized address |
| image_filename | MLS image filename |
| exif_artist | Photographer name from EXIF |
| exif_copyright | Copyright info from EXIF |
| exif_make | Camera brand |
| exif_model | Camera model |
| exif_lens_model | Lens model |
| exif_body_serial_number | Camera serial |
| exif_date_time_digitized | Photo timestamp |
| scraped_image_filename | Scraped photo caption |
| lp_flag | ListerPros indicator |
| cleaned | Cleaned flag |
| preferred_photographer | Known photographer |

## Use Cases

### Community Photos Verification
```javascript
// Check if email belongs to active Arizona agent
const response = await fetch('https://cdn.jsdelivr.net/.../verified_agents.json');
const data = await response.json();
const agent = data.agents.find(a => a.email === userEmail);
if (agent) {
  // Valid agent - send access link
}
```

### Listings Dashboard
The JSON files can power dashboards showing:
- Active listing counts by agent
- Market trends
- Camera/photographer usage analytics

## Local Development

```bash
# Process data locally
python process_data.py
```
