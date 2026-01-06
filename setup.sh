#!/bin/bash

# TacticX AI Data Crawling - Environment Setup Script
# This script sets up the Python environment after checking out the repository

set -e  # Exit on error

echo "🚀 Setting up TacticX AI Data Crawling environment..."

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

# Display Python version
PYTHON_VERSION=$(python3 --version)
echo "✓ Found $PYTHON_VERSION"

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi 

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "📥 Installing Python dependencies..."
pip install -r requirements.txt

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "⚠️  No .env file found"
    echo "📄 Creating .env from .env.example..."
    cp .env.example .env
    echo "✓ .env file created. Please update it with your actual configuration."
    echo "⚠️  IMPORTANT: Edit .env file with your Supabase credentials before running the application!"
else
    echo "✓ .env file already exists"
fi

# Create data directory if it doesn't exist
if [ ! -d "data/data_for_db" ]; then
    echo "📁 Creating data directory..."
    mkdir -p data/data_for_db
    echo "✓ Data directory created"
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "📝 Next steps:"
echo "   1. Edit .env file with your Supabase credentials"
echo "   2. Activate the virtual environment: source .venv/bin/activate"
echo "   3. Run tests: pytest -v tests/"
echo "   4. Run the application: python scripts/main.py"
echo ""
echo "📚 For more information, check the README.md file"
