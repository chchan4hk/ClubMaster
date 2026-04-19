FROM node:22-slim

LABEL "language"="nodejs"
LABEL "framework"="express"

ENV NODE_ENV=production
ENV PORT=8080
ENV SPORT_COACH_STATIC_ROOT=/app/static

WORKDIR /app

COPY . .

WORKDIR /app/src/backend

RUN npm install --include=dev

RUN npm run build

WORKDIR /app

# Create static directory and copy frontend files
RUN mkdir -p /app/static && \
    cp -r src/js /app/static/ 2>/dev/null || true && \
    cp -r src/css /app/static/ 2>/dev/null || true && \
    cp -r src/Source /app/static/ 2>/dev/null || true && \
    cp -r src/scripts /app/static/ 2>/dev/null || true && \
    cp src/*.html /app/static/ 2>/dev/null || true && \
    cp src/package.json /app/static/ 2>/dev/null || true

RUN chown -R node:node /app

USER node

EXPOSE 8080

CMD ["node", "src/backend/dist/server.js"]
