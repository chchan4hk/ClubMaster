# syntax=docker/dockerfile:1
#
# ClubMaster — build from the repository root (parent of `src/`):
#   docker build -t clubmaster:latest .
#   docker run --rm -e JWT_SECRET=your-secret -p 3000:3000 clubmaster:latest
#
# Production (NODE_ENV=production):
#   • JWT_SECRET is required (see src/backend/src/config/env.ts).
#   • PORT defaults to 3000 if unset (Zeabur/Railway usually inject PORT).
#   • Optional DB / RDS variables: see src/backend/.env.example
#
# Writable paths at runtime (bind-mount or volume for persistence):
#   • /app/data          — userLogin.json, BasicInfo.csv, coach/student logins, etc.
#   • /app/data_club     — club JSON/CSV/images (if you rely on on-disk writes)

ARG NODE_VERSION=20

# -----------------------------------------------------------------------------
# Dependencies + TypeScript compile
# -----------------------------------------------------------------------------
FROM node:${NODE_VERSION}-alpine AS builder

WORKDIR /build

COPY src/backend/package.json src/backend/package-lock.json ./
RUN npm ci

COPY src/backend/ ./
RUN npm run build

# -----------------------------------------------------------------------------
# Production image — backend at /app, static web root at /static
# -----------------------------------------------------------------------------
FROM node:${NODE_VERSION}-alpine AS production

RUN apk add --no-cache tini

ENV NODE_ENV=production \
    SPORT_COACH_STATIC_ROOT=/static

WORKDIR /app

COPY src/backend/package.json src/backend/package-lock.json ./
RUN npm ci --omit=dev

COPY --from=builder /build/dist ./dist
COPY --from=builder /build/data_club ./data_club
COPY --from=builder /build/lesson_modules ./lesson_modules
COPY --from=builder /build/payment_modules ./payment_modules
COPY --from=builder /build/coach_manager_modules ./coach_manager_modules
COPY --from=builder /build/student_modules ./student_modules
COPY --from=builder /build/prize_modules ./prize_modules
COPY --from=builder /build/certs ./certs

# Client HTML/JS/CSS and shared images (sibling of `backend/` in local dev — see server.ts)
COPY src/package.json /static/package.json
COPY src/js /static/js
COPY src/css /static/css
COPY src/Source /static/Source
COPY src/scripts /static/scripts
COPY src/*.html /static/

RUN mkdir -p /app/data/admin \
    && chown -R node:node /app /static

USER node

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD node -e "require('http').get('http://127.0.0.1:'+(process.env.PORT||3000)+'/api/health',r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/server.js"]
