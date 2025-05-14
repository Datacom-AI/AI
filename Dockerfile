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

RUN apk add --no-cache \
    chromium \
    nss \
    freetype \
    harfbuzz \
    ca-certificates \
    ttf-freefont

COPY package*.json ./
RUN npm install --production  # Chỉ lấy dependencies cần chạy

# Copy build kết quả từ stage 1
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/.env ./

EXPOSE 5000

CMD ["node", "dist/index.js"]
