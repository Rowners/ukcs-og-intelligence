import { NextRequest, NextResponse } from "next/server";

const OPERATOR_YF: Record<string, string> = {
  "HARBOUR ENERGY PLC": "HBR.L",
  "ENQUEST PLC":        "ENQ.L",
  "SERICA ENERGY":      "SQZ.L",
  "ITHACA ENERGY":      "ITH.L",
};

const GAS_TICKER   = "TTF=F";
const BRENT_TICKER = "BZ=F";
const YF_BASE      = "https://query1.finance.yahoo.com/v8/finance/chart";

// Mimic a real browser request to avoid Yahoo Finance server-side blocks
const YF_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Accept": "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9",
  "Origin": "https://finance.yahoo.com",
  "Referer": "https://finance.yahoo.com/",
};

function dateKey(ts: number): string {
  return new Date(ts * 1000).toISOString().slice(0, 10);
}

async function fetchChart(
  symbol: string,
  start: number,
  end: number
): Promise<Map<string, number>> {
  const url = `${YF_BASE}/${encodeURIComponent(symbol)}?interval=1wk&period1=${start}&period2=${end}`;
  try {
    const res = await fetch(url, { headers: YF_HEADERS });
    if (!res.ok) {
      console.warn(`YF ${symbol}: HTTP ${res.status} ${res.statusText}`);
      return new Map();
    }
    const json = await res.json();
    const result = json?.chart?.result?.[0];
    if (!result) {
      const err = json?.chart?.error;
      console.warn(`YF ${symbol}: no result. Error:`, JSON.stringify(err));
      return new Map();
    }
    const timestamps: number[]      = result.timestamp ?? [];
    const closes: (number | null)[] = result.indicators?.quote?.[0]?.close ?? [];
    const map = new Map<string, number>();
    timestamps.forEach((ts, i) => {
      const c = closes[i];
      if (c != null) map.set(dateKey(ts), c);
    });
    return map;
  } catch (e) {
    console.warn(`YF ${symbol} fetch error:`, e);
    return new Map();
  }
}

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const operator = searchParams.get("operator") || "HARBOUR ENERGY PLC";
  const days     = parseInt(searchParams.get("days") || "365");

  const yf_symbol = OPERATOR_YF[operator];
  if (!yf_symbol) {
    return NextResponse.json({ error: "Unknown operator" }, { status: 400 });
  }

  const end   = Math.floor(Date.now() / 1000);
  const start = Math.floor(end - days * 86400);

  const [shareMap, brentMap, gasMap] = await Promise.all([
    fetchChart(yf_symbol,    start, end),
    fetchChart(BRENT_TICKER, start, end),
    fetchChart(GAS_TICKER,   start, end),
  ]);

  if (shareMap.size === 0) {
    return NextResponse.json({ error: `No data for ${yf_symbol}` }, { status: 502 });
  }

  const raw: { date: string; share: number; brent: number | null; gas: number | null }[] = [];
  for (const [date, share] of Array.from(shareMap.entries())) {
    raw.push({
      date,
      share,
      brent: brentMap.get(date) ?? null,
      gas:   gasMap.get(date)   ?? null,
    });
  }
  raw.sort((a, b) => a.date.localeCompare(b.date));

  if (raw.length === 0) return NextResponse.json([]);

  const firstShare = raw[0].share;
  const firstBrent = raw.find(p => p.brent != null)?.brent ?? 1;
  const firstGas   = raw.find(p => p.gas   != null)?.gas   ?? 1;

  const indexed = raw.map(p => ({
    date:  p.date,
    share: parseFloat(((p.share  / firstShare) * 100).toFixed(1)),
    brent: p.brent != null ? parseFloat(((p.brent / firstBrent) * 100).toFixed(1)) : null,
    gas:   p.gas   != null ? parseFloat(((p.gas   / firstGas)   * 100).toFixed(1)) : null,
  }));

  return NextResponse.json(indexed);
}
