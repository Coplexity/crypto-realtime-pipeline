'use client';

import { useEffect, useState } from 'react';

type Trade = {
  price: string;
  quantity: string;
  timestamp: number;
};

export default function Dashboard() {
  const [trade, setTrade] = useState<Trade | null>(null);

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8000/ws/crypto');
    ws.onmessage = (event) => {
      setTrade(JSON.parse(event.data));
    };
    return () => ws.close();
  }, []);

  return (
    <div className="p-10 text-center">
      <h1 className="text-2xl font-bold">HUST Crypto Monitor - Phase 1</h1>
      {trade ? (
        <div className="mt-5 rounded-lg border p-5 shadow-lg">
          <p className="font-mono text-4xl text-green-500">${trade.price}</p>
          <p className="text-gray-500">Volume: {trade.quantity}</p>
          <p className="text-sm">Time: {new Date(trade.timestamp).toLocaleTimeString()}</p>
        </div>
      ) : (
        <p>Dang doi du lieu tu Kafka...</p>
      )}
    </div>
  );
}
