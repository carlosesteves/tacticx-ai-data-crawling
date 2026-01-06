# tacticx-ai-data-crawling
Data crawling for Tacticx.ai

## 🚀 Quick Setup

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)
- Supabase account and credentials

### Automated Setup (Recommended)

1. Clone the repository:
```bash
git clone <repository-url>
cd tacticx-ai-data-crawling
```

2. Run the setup script:
```bash
./setup.sh
```

3. Edit the `.env` file with your Supabase credentials:
```bash
nano .env  # or use your preferred editor
```

4. Activate the virtual environment:
```bash
source .venv/bin/activate
```

### Manual Setup

If you prefer to set up manually:

1. Create and activate a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create environment configuration:
```bash
cp .env.example .env
# Edit .env with your actual configuration
```

4. Create data directories:
```bash
mkdir -p data/data_for_db
```

## 🧪 Running Tests

```bash
pytest -v tests/
```

## 🏃 Running the Application

```bash
# Update 2025 matches
python scripts/main.py

# Update all leagues for season 2025
python scripts/update_all_leagues_season.py --season 2025
```

## 📁 Project Structure

- `config/` - Configuration files and constants
- `models/` - Data models (Coach, Match, League, etc.)
- `pages/` - Web scraping page handlers
- `pipelines/` - Data processing pipelines
- `repositories/` - Database interaction layer
- `scripts/` - Utility scripts for data operations
- `services/` - Business logic services
- `tests/` - Test suite
- `utils/` - Helper utilities

## 🔧 Environment Variables

The following environment variables need to be configured in `.env`:

- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_KEY` - Your Supabase anon/public key
- `CLUB_DATA_PATH` - Path to club data CSV file
- `ODDS_API_URL` - The Odds API base URL

## 📝 Available Tasks

The project includes VS Code tasks:
- **Update 2025 matches** - Run tests and update match data
- **Update All Leagues Season 2025** - Update all leagues for the 2025 season

