# Based on the official Odoo 18 Community image (includes requests).
FROM odoo:18.0

USER root
RUN export LC_ALL=C.UTF-8 DEBIAN_FRONTEND=noninteractive \
    && apt-get update -o APT::Update::Error-Mode=any \
    && apt-get upgrade --yes \
    && rm -rf /var/lib/apt/lists/*
USER odoo
