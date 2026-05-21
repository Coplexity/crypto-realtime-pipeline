import axios from "axios"

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"

const api = axios.create({ baseURL: API_URL, timeout: 10000 })

export const fetchOHLC = async (symbol: string, interval: string, limit = 200) => {
  const { data } = await api.get("/ohlc", { params: { symbol, interval, limit } })
  return data
}

// Đã sửa thành truyền params thay vì nhét thẳng vào URL
export const fetchOrderbook = async (symbol: string) => {
  const { data } = await api.get("/orderbook", { params: { symbol } })
  return data
}

// Đã sửa thành truyền params thay vì nhét thẳng vào URL
export const fetchTrades = async (symbol: string, limit = 30) => {
  const { data } = await api.get("/trades", { params: { symbol, limit } })
  return data
}

export const fetchPredictions = async () => {
  const { data } = await api.get("/predictions")
  return data
}

export const fetchSymbols = async () => {
  const { data } = await api.get("/symbols")
  return data
}

export const fetchTopGainers = async (type = "gainers", limit = 10) => {
  const { data } = await api.get("/ranking/top-gainers", { params: { type, limit } })
  return data
}

export const fetchPredictionHistory = async (symbol: string, limit = 50) => {
  const { data } = await api.get(`/prediction/${symbol}/history`, { params: { limit } })
  return data
}

export const fetchMarketRanking = async (type: "gainers" | "losers" = "gainers", limit = 100) => {
  const { data } = await api.get("/ranking/top-gainers", { params: { type, limit } })
  return data
}