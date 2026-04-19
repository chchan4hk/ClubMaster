FROM node:22-slim

LABEL "language"="nodejs"

WORKDIR /app

# Copy all source files first
COPY . .

# Install dependencies from the backend directory
WORKDIR /app/src/backend
RUN npm install

# Build the TypeScript backend
RUN npm run build

# Go back to app root for runtime
WORKDIR /app

# Create static directory and copy backend-served HTML modules + web root (js/css/html)
RUN mkdir -p /app/static && \
    cp -r src/backend/coach_manager_modules /app/static/ 2>/dev/null || true && \
    cp -r src/backend/lesson_modules /app/static/ 2>/dev/null || true && \
    cp -r src/backend/payment_modules /app/static/ 2>/dev/null || true && \
    cp -r src/backend/prize_modules /app/static/ 2>/dev/null || true && \
    cp -r src/backend/student_modules /app/static/ 2>/dev/null || true && \
    cp -r src/js /app/static/ 2>/dev/null || true && \
    cp -r src/css /app/static/ 2>/dev/null || true && \
    cp -r src/Source /app/static/ 2>/dev/null || true && \
    cp -r src/scripts /app/static/ 2>/dev/null || true && \
    cp src/package.json /app/static/ 2>/dev/null || true && \
    cp src/*.html /app/static/ 2>/dev/null || true

# Set environment variables
ENV PORT=3000
ENV SPORT_COACH_STATIC_ROOT=/app/static

# Ensure proper permissions
RUN chown -R node:node /app

USER node

EXPOSE 3000

CMD ["node", "src/backend/dist/server.js"]
