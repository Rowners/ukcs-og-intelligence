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

const YF_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Accept": "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9",
  "Origin": "https://finance.yahoo.com",
  "Referer": "https://finance.yahoo.com/",
};

export interface Series {
  date:  string;
  value: number;
}

function dateKey(ts: number): string {
  return new Date(ts * 1000).toISOString().slice(0, 10);
}

async function fetchChart(symbol: string, start: number, end: number): Promise<Series[]> {
  const url = `${YF_BASE}/${encodeURIComponent(symbol)}?interval=1wk&period1=${start}&period2=${end}`;
  try {
    const res = await fetch(url, { headers: YF_HEADERS });
    if (!res.ok) {
      console.warn(`YF ${symbol}: HTTP ${res.status}`);
      return [];
    }
    const json = await res.json();
    const result = json?.chart?.result?.[0];
    if (!result) {
      console.warn(`YF ${symbol}: no result`, JSON.stringify(json?.chart?.error));
      return [];
    }
    const timestamps: number[]      = result.timestamp ?? [];
    const closes: (number | null)[] = result.indicators?.quote?.[0]?.close ?? [];
    const points: Series[] = [];
    timestamps.forEach((ts, i) => {
      const c = closes[i];
      if (c != null) points.push({ date: dateKey(ts), value: parseFloat(c.toFixed(2)) });
    });
    return points.sort((a, b) => a.date.localeCompare(b.date));
  } catch (e) {
    console.warn(`YF ${symbol} fetch error:`, e);
    return [];
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

  const [share, brent, gas] = await Promise.all([
    fetchChart(yf_symbol,    start, end),
    fetchChart(BRENT_TICKER, start, end),
    fetchChart(GAS_TICKER,   start, end),
  ]);

  if (share.length === 0) {
    return NextResponse.json({ error: `No data for ${yf_symbol}` }, { status: 502 });
  }

  return NextResponse.json({ share, brent, gas });
}
