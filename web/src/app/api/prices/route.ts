import { NextRequest, NextResponse } from "next/server";
import yahooFinance from "yahoo-finance2";

// Suppress yahoo-finance2 validation notices that would otherwise throw
yahooFinance.setGlobalConfig({ validation: { logErrors: false } });

interface YFRow {
  date: Date;
  close: number | null;
}

// yahoo-finance2 v3 overloads don't resolve cleanly with interval — cast to bypass
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const yfHistorical = yahooFinance.historical as (
  symbol: string,
  opts: { period1: Date; period2: Date; interval: string },
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  moduleOpts?: any
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

/** Fetch one ticker, returning [] on failure rather than throwing. */
async function safeFetch(symbol: string, period1: Date, period2: Date): Promise<YFRow[]> {
  try {
    return await yfHistorical(symbol, { period1, period2, interval: "1wk" }, { validateResult: false });
  } catch (e) {
    console.warn(`yfHistorical failed for ${symbol}:`, e);
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

  const end   = new Date();
  const start = new Date(end);
  start.setDate(start.getDate() - days);

  try {
    const [shareQuotes, brentQuotes, gasQuotes] = await Promise.all([
      safeFetch(yf_symbol,   start, end),
      safeFetch(BRENT_TICKER, start, end),
      safeFetch(GAS_TICKER,   start, end),
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

    if (shareQuotes.length === 0) {
      return NextResponse.json({ error: `No data returned for ${yf_symbol}` }, { status: 502 });
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
    const msg = err instanceof Error ? err.message : String(err);
    console.error("Prices API error:", msg);
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}
