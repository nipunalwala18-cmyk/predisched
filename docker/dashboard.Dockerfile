# The dashboard of prompt 23: built static files served by nginx, which also proxies the
# dashboard API (/api, /ws, /swagger-ui) so the browser talks to one origin.
#
#   docker build -f docker/dashboard.Dockerfile -t predisched-dashboard .
FROM node:20-alpine AS build
WORKDIR /src
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
# Empty: the API is on the page's own origin, behind nginx.
ENV VITE_API_URL=""
RUN npm run build

# Runs as the non-root nginx user and listens on 8080.
FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY docker/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /src/dist /usr/share/nginx/html
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=12 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/ || exit 1
