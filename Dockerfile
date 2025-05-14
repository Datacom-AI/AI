# ---------- STAGE 1: Build ----------
FROM node:20-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm install  # Cài cả devDependencies (build được TypeScript)

COPY . .

RUN npm run build

# ---------- STAGE 2: Runtime ----------
FROM node:20-alpine

WORKDIR /app

RUN apt-get update && apt-get install -y \
    chromium-browser \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    --no-install-recommends

COPY package*.json ./
RUN npm install --production  # Chỉ lấy dependencies cần chạy

# Copy build kết quả từ stage 1
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/.env ./

EXPOSE 5000

CMD ["node", "dist/index.js"]
