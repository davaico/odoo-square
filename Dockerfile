# Odoo Square Integration - Docker Image
# Based on the official Odoo 17 Community image
FROM odoo:17

USER root

# Install system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies (requests is already included in Odoo image)
# RUN pip3 install --no-cache-dir requests

# Copy entrypoint script
COPY --chown=odoo:odoo --chmod=0755 ./entrypoint.sh /entrypoint.sh

# Switch back to odoo user
USER odoo

ENTRYPOINT ["/entrypoint.sh"]
CMD ["odoo"]
