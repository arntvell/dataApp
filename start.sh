#!/bin/bash

echo "🚀 Starting DataApp - Sitoo & Shopify Integration Platform"
echo "=========================================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp env.example .env
    echo "⚠️  Please edit .env file with your actual API credentials before continuing."
    echo "   - SITOO_API_KEY"
    echo "   - SHOPIFY_API_KEY"
    echo "   - SHOPIFY_PASSWORD"
    echo ""
    read -p "Press Enter after updating .env file..."
fi

echo "🔨 Building Docker containers..."
docker-compose build

echo "🚀 Starting services..."
docker-compose up -d

echo "⏳ Waiting for services to start..."
sleep 10

echo "🔍 Checking service status..."
docker-compose ps

echo ""
echo "✅ DataApp is starting up!"
echo ""
echo "📊 API Documentation: http://localhost:8000/docs"
echo "🏥 Health Check: http://localhost:8000/health"
echo "📝 View logs: docker-compose logs -f"
echo "🛑 Stop services: docker-compose down"
echo ""
echo "🎯 Next steps:"
echo "1. Wait for services to fully start (check logs if needed)"
echo "2. Test the setup: python scripts/test_setup.py"
echo "3. Initialize database: python scripts/init_db.py"
echo "4. Run first sync: python scripts/run_sync.py"



