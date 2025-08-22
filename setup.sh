#!/bin/bash

# Data Ingestion System Setup Script
# This script sets up the data ingestion system

echo "Setting up Data Ingestion System..."

# Create necessary directories
echo "Creating directories..."
mkdir -p raw_data
mkdir -p processed_data
mkdir -p logs

# Install required packages
echo "Installing required packages..."
pip install -r data_ingestion/requirements.txt

# Set up Kaggle API (optional)
if [ ! -f ~/.kaggle/kaggle.json ]; then
    echo "Kaggle API not configured. To use Kaggle datasets:"
    echo "1. Go to https://www.kaggle.com/account"
    echo "2. Create new API token"
    echo "3. Move kaggle.json to ~/.kaggle/kaggle.json"
    echo "4. chmod 600 ~/.kaggle/kaggle.json"
fi

# Create log rotation script
cat > setup_log_rotation.sh << 'EOF'
#!/bin/bash
# Log rotation for data ingestion logs
# Add this to cron for automatic log rotation

LOG_DIR="/Users/I528946/Desktop/Use cases/use case 1/AiImageDetection/logs"
MAX_AGE=30  # days

# Remove logs older than MAX_AGE days
find "$LOG_DIR" -name "*.log" -mtime +$MAX_AGE -delete

# Compress logs older than 7 days
find "$LOG_DIR" -name "*.log" -mtime +7 ! -name "*.gz" -exec gzip {} \;
EOF

chmod +x setup_log_rotation.sh

# Create systemd service file (for Linux)
cat > data_ingestion.service << 'EOF'
[Unit]
Description=Data Ingestion Service
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/Users/I528946/Desktop/Use cases/use case 1/AiImageDetection
ExecStart=/usr/bin/python3 -m data_ingestion.main schedule
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo "Setup complete!"
echo ""
echo "To start the ingestion system:"
echo "  python -m data_ingestion.main run          # Run once"
echo "  python -m data_ingestion.main schedule     # Run as service"
echo "  python -m data_ingestion.main status       # Check status"
echo "  python -m data_ingestion.main list         # List sources"
echo ""
echo "To run a specific source:"
echo "  python -m data_ingestion.main run --source churn_data"
