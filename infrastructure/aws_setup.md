# 🚀 AWS Deployment Guide — FEMA ML System

This guide deploys the system using:

- EC2 (compute)
- S3 (data + model storage)
- Docker (services)
- Optional: ECR (container registry)

---

# 🧱 1. PREREQUISITES

- AWS account
- AWS CLI installed
- Docker installed locally
- SSH key pair created

---

# 🔐 2. CONFIGURE AWS CLI

```bash
aws configure



nter:

Access Key

Secret Key

Region: eu-west-2

Output: json


3. CREATE S3 BUCKET
aws s3 mb s3://fema-ml-bucket

Upload model + data:

aws s3 cp models/ s3://fema-ml-bucket/models/ --recursive
aws s3 cp data/ s3://fema-ml-bucket/data/ --recursive
🖥️ 4. LAUNCH EC2 INSTANCE

Recommended:

Instance type: t3.medium

OS: Ubuntu 22.04

Storage: 20GB+

Open ports:

22 (SSH)

8000 (API)

8501 (Dashboard)

8080 (Airflow)

🔑 5. CONNECT TO EC2
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>
⚙️ 6. INSTALL DOCKER
sudo apt update
sudo apt install -y docker.io docker-compose
sudo usermod -aG docker ubuntu
newgrp docker
📥 7. CLONE PROJECT
git clone https://github.com/YOUR_USERNAME/fema-ml-system.git
cd fema-ml-system
📦 8. DOWNLOAD DATA FROM S3
aws s3 cp s3://fema-ml-bucket/models/ ./models/ --recursive
aws s3 cp s3://fema-ml-bucket/data/ ./data/ --recursive
▶️ 9. RUN SYSTEM
docker-compose up -d --build
🌐 10. ACCESS SERVICES

API → http://<EC2_IP>:8000/docs

Dashboard → http://<EC2_IP>:8501

Airflow → http://<EC2_IP>:8080

🐳 11. OPTIONAL: PUSH TO ECR
Create repo
aws ecr create-repository --repository-name fema-api
Login
aws ecr get-login-password | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.eu-west-2.amazonaws.com
Tag + push
docker tag fema-api:latest <ECR_URL>/fema-api:latest
docker push <ECR_URL>/fema-api:latest

🔁 12. AUTO-START CONTAINERS (OPTIONAL)
sudo systemctl enable docker

📊 13. MONITORING (OPTIONAL)

Install:

docker run -d -p 9090:9090 prom/prometheus
docker run -d -p 3000:3000 grafana/grafana
🧠 ARCHITECTURE SUMMARY

User → EC2 → Docker → FastAPI → Model
→ Streamlit
→ Airflow

Data → S3
Models → S3

🔥 PRODUCTION IMPROVEMENTS

Use ALB (Load Balancer)

Use RDS instead of local DB

Use ECS or EKS instead of EC2

Use IAM roles instead of keys


---

# ⚡ OPTIONAL (AUTOMATION SCRIPT)

## 📄 `infrastructure/deploy.sh`

```bash
#!/bin/bash

echo "🚀 Starting deployment..."

# Pull latest code
git pull

# Stop old containers
docker-compose down

# Rebuild and run
docker-compose up -d --build

echo "✅ Deployment complete"
🔐 OPTIONAL: .env FOR AWS
📄 .env
AWS_REGION=eu-west-2
S3_BUCKET=fema-ml-bucket
🧠 FINAL RESULT

You now have:

✅ Real cloud deployment workflow
✅ Data + model storage (S3)
✅ Compute (EC2)
✅ Container orchestration (Docker)
✅ Optional registry (ECR)