# -------------------------------
    # -------------------------------
        # BASE IMAGE
        # -------------------------------
        FROM python:3.10-slim
        
        # -------------------------------
        # ENV VARIABLES
        # -------------------------------
        ENV PYTHONUNBUFFERED=1
        ENV PIP_NO_CACHE_DIR=1
        
        # -------------------------------
        # SYSTEM DEPENDENCIES
        # -------------------------------
        RUN apt-get update && apt-get install -y \
            build-essential \
            curl \
            && rm -rf /var/lib/apt/lists/*
        
        # -------------------------------
        # WORKDIR
        # -------------------------------
        WORKDIR /app
        
        # -------------------------------
        # INSTALL PYTHON DEPENDENCIES
        # -------------------------------
        COPY requirements.txt .
        
        RUN pip install --upgrade pip \
            && pip install -r requirements.txt
        
        # -------------------------------
        # COPY APPLICATION
        # -------------------------------
        COPY dashboard/ .
        
        # -------------------------------
        # SECURITY (NON-ROOT USER)
        # -------------------------------
        RUN useradd -m appuser
        USER appuser
        
        # -------------------------------
        # EXPOSE PORT
        # -------------------------------
        EXPOSE 8501
        
        # -------------------------------
        # HEALTHCHECK
        # -------------------------------
        HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1
        
        # -------------------------------
        # RUN STREAMLIT
        # -------------------------------
        CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]