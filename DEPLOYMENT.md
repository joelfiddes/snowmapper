# SnowMapper Deployment Guide

## Local Development (Conda)

```bash
# Create environment
conda env create -f environment.yml

# Activate
conda activate snowmapper

# Run pipeline
cd /path/to/simulation
bash run_2000.sh
```

## Docker Deployment

### Build Image

```bash
cd /path/to/snowmapper
docker build -t snowmapper:latest .
```

### Run Container

```bash
# Interactive mode
docker run -it -v /path/to/data:/data snowmapper bash

# Run pipeline directly
docker run -v /path/to/data:/data snowmapper bash /data/run_2000.sh

# With AWS credentials for uploads
docker run -v /path/to/data:/data -v ~/.aws:/home/mambauser/.aws:ro snowmapper bash /data/run_2000.sh
```

### Docker Compose

```bash
# Set data directory
export SNOWMAPPER_DATA=/path/to/simulation

# Run
docker-compose up snowmapper

# With scheduler (daily at 6 AM UTC)
docker-compose up -d
```

## AWS EC2 Deployment

### 1. Launch Instance

- **AMI**: Ubuntu 22.04 LTS
- **Instance type**: r5.xlarge (4 vCPU, 32 GB RAM) or larger
- **Storage**: 100+ GB EBS

### 2. Install Docker

```bash
sudo apt update
sudo apt install -y docker.io docker-compose
sudo usermod -aG docker $USER
```

### 3. Deploy

```bash
# Clone repo
git clone https://github.com/your-org/snowmapper.git
cd snowmapper

# Build
docker build -t snowmapper:latest .

# Set up data directory
mkdir -p /data/snowmapper_2026
# Copy or sync input data...

# Run
docker run -v /data/snowmapper_2026:/data snowmapper bash /data/run_2000.sh
```

### 4. Schedule with Cron

```bash
# Edit crontab
crontab -e

# Add daily run at 6 AM UTC
0 6 * * * docker run --rm -v /data/snowmapper_2026:/data snowmapper bash /data/run_2000.sh >> /var/log/snowmapper.log 2>&1
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SNOWMAPPER_LOG_LEVEL` | `INFO` | Logging verbosity (DEBUG, INFO, WARNING, ERROR) |
| `AWS_DEFAULT_REGION` | `eu-central-1` | AWS region for S3 uploads |

## FSM Binary

The FSM (Factorial Snow Model) binary must be available at `./FSM` in each domain directory.

### Option 1: Copy pre-built binary

```bash
cp /path/to/FSM /data/D2000/FSM
chmod +x /data/D2000/FSM
```

### Option 2: Compile from source

```bash
git clone https://github.com/RichardEssery/FSM.git
cd FSM
gfortran -o FSM src/*.F90
cp FSM /data/D2000/
```

## Monitoring

Check logs:
```bash
# Pipeline logs
tail -f /data/logs/pipeline_*.log

# Domain-specific logs
tail -f /data/D2000/logs/*.log
```

## Troubleshooting

### Missing cdo
```bash
conda install -c conda-forge cdo
```

### ECMWF API errors
Check that forecast data is available (~9 hours after 00Z/12Z runs).

### Memory issues
Increase container memory limit or use a larger instance.
