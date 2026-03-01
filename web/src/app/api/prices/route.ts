import { NextRequest, NextResponse } from "next/server";
import yahooFinance from "yahoo-finance2";

interface YFRow {
  date: Date;
  close: number | null;
}

// yahoo-finance2 v3 overloads don't resolve cleanly with interval — cast to bypass
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const yfHistorical = yahooFinance.historical as (
  symbol: string,
  opts: { period1: Date; period2: Date; interval: string }
) => Promise<YFRow[]>;

const OPERATOR_YF: Record<string, string> = {
  "HARBOUR ENERGY PLC": "HBR.L",
  "ENQUEST PLC":        "ENQ.L",
  "SERICA ENERGY":      "SQZ.L",
  "ITHACA ENERGY":      "ITH.L",
};

// TTF Natural Gas Futures (Dutch Title Transfer Facility) — European gas benchmark
const GAS_TICKER   = "TTF=F";
const BRENT_TICKER = "BZ=F";

function dateKey(d: Date): string {
  return d.toISOString().slice(0, 10);
}

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const operator = searchParams.get("operator") || "HARBOUR ENERGY PLC";
  const days     = parseInt(searchParams.get("days") || "365");

  const yf_symbol = OPERATOR_YF[operator];
  if (!yf_symbol) {
    return NextResponse.json({ error: "Unknown operator" }, { status: 400 });
  }

  const end   = new Date();
  const start = new Date(end);
  start.setDate(start.getDate() - days);

  try {
    const opts = { period1: start, period2: end, interval: "1wk" as const };

    const [shareQuotes, brentQuotes, gasQuotes] = await Promise.all([
      yfHistorical(yf_symbol,   opts),
      yfHistorical(BRENT_TICKER, opts),
      yfHistorical(GAS_TICKER,   opts),
    ]);

    // Build date-keyed maps for commodity prices
    const brentMap: Record<string, number> = {};
    const gasMap:   Record<string, number> = {};

    for (const q of brentQuotes) {
      if (q.date && q.close != null) brentMap[dateKey(q.date)] = q.close;
    }
    for (const q of gasQuotes) {
      if (q.date && q.close != null) gasMap[dateKey(q.date)] = q.close;
    }

    // Align all series to share price dates
    const raw = shareQuotes
      .filter(q => q.date && q.close != null)
      .map(q => ({
        date:  dateKey(q.date),
        share: q.close as number,
        brent: brentMap[dateKey(q.date)] ?? null,
        gas:   gasMap[dateKey(q.date)]   ?? null,
      }));

    if (raw.length === 0) return NextResponse.json([]);

    // Normalise all series to 100 at first valid point
    const firstShare = raw[0].share;
    const firstBrent = raw.find(p => p.brent != null)?.brent ?? 1;
    const firstGas   = raw.find(p => p.gas   != null)?.gas   ?? 1;

    const indexed = raw.map(p => ({
      date:  p.date,
      share: parseFloat(((p.share / firstShare) * 100).toFixed(1)),
      brent: p.brent != null ? parseFloat(((p.brent / firstBrent) * 100).toFixed(1)) : null,
      gas:   p.gas   != null ? parseFloat(((p.gas   / firstGas)   * 100).toFixed(1)) : null,
    }));

    return NextResponse.json(indexed);
  } catch (err) {
    console.error("Prices API error:", err);
    return NextResponse.json({ error: "Failed to load price data" }, { status: 500 });
  }
}
