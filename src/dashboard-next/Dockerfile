FROM node:20-alpine AS builder

WORKDIR /app

COPY src/dashboard-next/package*.json ./
RUN npm ci

COPY src/dashboard-next/ .

ENV API_URL=http://api:8000
ENV NEXT_PUBLIC_API_URL=http://api:8000
RUN npm run build

# ── Production ──
FROM node:20-alpine AS runner

WORKDIR /app
ENV NODE_ENV=production
ENV API_URL=http://api:8000

RUN addgroup --system --gid 1001 nodejs && adduser --system --uid 1001 nextjs

COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs
EXPOSE 3000
ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

CMD ["node", "server.js"]
