# Odoo Square Integration - Docker Image
# Based on the official Odoo 17 Community image
FROM odoo:17.0

USER root

# Install system packages
RUN export LC_ALL=C.UTF-8 DEBIAN_FRONTEND=noninteractive \
    && apt-get update -o APT::Update::Error-Mode=any \
    && apt-get upgrade --yes \
    && apt-get install --yes --no-install-recommends python3-pip postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (requests is already included in Odoo image)
# RUN pip3 install --no-cache-dir requests

# Copy entrypoint script
COPY --chown=odoo:odoo --chmod=0755 ./entrypoint.sh /entrypoint.sh

# Switch back to odoo user
USER odoo

ENTRYPOINT ["/entrypoint.sh"]
CMD ["odoo"]
