# Frontend - Phase 5: Visualization Dashboard

Next.js 14 dashboard untuk visualize dữ liệu tiền điện tử real-time.
Kết nối tới FastAPI backend qua WebSocket để stream trade data live.

## Kiến Trúc

```
frontend/
├── app/
│   ├── layout.tsx        # Root layout + providers
│   ├── page.tsx          # Home page
│   └── globals.css       # Global styles
├── components/           # React components (Phase 5)
├── lib/                  # Utilities (Phase 5)
└── public/               # Static assets
```

## Tính Năng

### Phase 5 Planned:
- Real-time trade data stream via WebSocket
- Interactive OHLCV candlestick charts
- Market statistics dashboard
- Price alerts and notifications
- Historical data visualization

## Setup

```bash
# Install dependencies
npm install

# Development server
npm run dev

# Production build
npm run build
npm run start
```

## Environment

Create `.env.local`:
```
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000
```

## Technology Stack

- **Framework:** Next.js 14 (App Router)
- **Styling:** Tailwind CSS + PostCSS
- **Charts:** D3.js / Recharts (Phase 5)
- **State:** React Hooks / Context API (Phase 5)
- **WebSocket:** Native WebSocket API
- **HTTP Client:** Fetch API

## Development

Development server runs on http://localhost:3000

```bash
npm run dev
```

## Production

```bash
npm run build
npm run start
```

## Notes

Phase 5 is reserved for dashboard implementation.
Backend API (FastAPI) should be running on http://localhost:8000


## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
