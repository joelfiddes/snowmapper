# SnowMapper Operational Container
# Generates daily snow maps for High Mountain Central Asia

FROM mambaorg/micromamba:1.5-jammy

LABEL maintainer="joel@snowmapper.ch"
LABEL description="SnowMapper snow mapping pipeline"

# Set environment variables
ENV MAMBA_ROOT_PREFIX=/opt/conda
ENV PATH=/opt/conda/envs/snowmapper/bin:$PATH
ENV SNOWMAPPER_HOME=/app
ENV SNOWMAPPER_LOG_LEVEL=INFO

# Copy environment file
COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml

# Create conda environment
RUN micromamba create -y -f /tmp/environment.yml && \
    micromamba clean --all --yes

# Switch to root for system packages
USER root

# Install gfortran for FSM compilation (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gfortran \
    make \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy snowmapper code
COPY --chown=$MAMBA_USER:$MAMBA_USER . /app/snowmapper/

# Copy pre-built FSM binary (alternatively, compile from source)
# COPY --chown=$MAMBA_USER:$MAMBA_USER FSM /app/bin/FSM
# RUN chmod +x /app/bin/FSM

# Create directories for data volumes
RUN mkdir -p /data/inputs /data/outputs /data/logs && \
    chown -R $MAMBA_USER:$MAMBA_USER /data

# Switch back to non-root user
USER $MAMBA_USER

# Activate environment by default
ARG MAMBA_DOCKERFILE_ACTIVATE=1

# Set Python path
ENV PYTHONPATH=/app/snowmapper:$PYTHONPATH

# Default working directory for simulations
WORKDIR /data

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import xarray; import TopoPyScale" || exit 1

# Default command: show help
CMD ["python", "-c", "print('SnowMapper container ready. Mount data volume and run pipeline.')"]
