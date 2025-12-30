#!/bin/bash

# Build and deploy script for Render

echo "🚀 Starting deployment..."

# Check environment variables
if [ -z "$BOT_TOKEN" ]; then
    echo "❌ Error: BOT_TOKEN not set"
    exit 1
fi

echo "📦 Building Docker image..."
docker build -t telegram-referral-bot .

echo "🐳 Starting containers..."
docker-compose up -d

echo "✅ Deployment complete!"
echo ""
echo "📋 Next steps:"
echo "1. Add channels using: /addchannel <chat_id> <invite_link> <title>"
echo "2. Test channel access: /testchannel <chat_id>"
echo "3. Start promoting your bot!"
