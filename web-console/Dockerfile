# Stage 1: Build frontend
FROM node:20-alpine AS frontend-build
WORKDIR /app
COPY package.json ./
RUN npm install --frozen-lockfile || npm install
COPY tsconfig.json vite.config.ts vite-env.d.ts index.html ./
COPY src/ ./src/
COPY public/ ./public/
RUN npm run build

# Stage 2: Build BFF
FROM node:20-alpine AS bff-build
WORKDIR /app
COPY server/package.json ./
RUN npm install --frozen-lockfile || npm install
COPY server/tsconfig.json ./
COPY server/src/ ./src/
RUN npx tsc || true

# Stage 3: Production
FROM node:20-alpine
RUN apk add --no-cache nginx

WORKDIR /app

# BFF
COPY --from=bff-build /app/node_modules ./node_modules
COPY --from=bff-build /app/src ./src
COPY server/package.json ./

# Frontend static files
COPY --from=frontend-build /app/dist /usr/share/nginx/html

# Nginx config
COPY nginx.conf /etc/nginx/http.d/default.conf

# Upload directory
RUN mkdir -p /tmp/imgsrch-uploads

# Start script
COPY <<'EOF' /app/start.sh
#!/bin/sh
nginx
exec node --import tsx src/index.ts
EOF
RUN chmod +x /app/start.sh

EXPOSE 3000

CMD ["/app/start.sh"]
