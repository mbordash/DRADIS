# The TLS terminator in front of the Control Tower (see nginx.conf). Built at
# AMI-build time instead of pulling nginx:alpine as published: the official
# image lags Alpine's security fixes by days or weeks, and the Marketplace
# scanner refuses an AMI whose baked images carry a CVE Alpine has already
# fixed (2026-10-31: brotli-libs 1.2.0-r1, CVE-2026-111497, fixed in 1.2.1-r0).
# `apk upgrade` takes every fix in Alpine's current package set; nothing else
# about the image changes, and nginx.conf is still mounted by docker-compose.
FROM nginx:alpine
RUN apk upgrade --no-cache
