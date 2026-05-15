import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Crypto Analytics Dashboard",
  description: "Real-time cryptocurrency analytics platform",
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet" />
      </head>
      <body style={{ margin: 0, background: "#0a0e1a", fontFamily: "'JetBrains Mono', monospace" }}>
        {children}
      </body>
    </html>
  )
}
