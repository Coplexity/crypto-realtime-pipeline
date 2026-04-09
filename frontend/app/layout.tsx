import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "CryptoStream Dashboard — HUST Big Data",
  description:
    "Real-time cryptocurrency trading dashboard built with Lambda Architecture. Powered by Apache Kafka, Spark Structured Streaming, MongoDB, and Next.js.",
  keywords: ["cryptocurrency", "bitcoin", "trading", "dashboard", "big data", "spark", "kafka"],
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="vi"
      className={`${geistSans.variable} ${geistMono.variable}`}
    >
      <body>{children}</body>
    </html>
  );
}
