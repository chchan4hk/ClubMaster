# Twelve-factor: build from repo root — `docker build -t sport-coach .`
# Runtime PORT comes from the platform (Zeabur, Docker `-e`, Kubernetes); do not bake it in here.
FROM node:20-alpine
WORKDIR /app

COPY src/backend/package.json src/backend/package-lock.json ./
RUN npm ci

COPY src/backend/ ./

ENV NODE_ENV=production
RUN npm run build && npm prune --omit=dev

USER node
CMD ["node", "dist/server.js"]
