"use client";

import { useEffect, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";

interface PricePoint {
  date: string;
  share: number | null;
  brent: number | null;
  gas: number | null;
}

const OPERATORS = [
  { label: "Harbour Energy", value: "HARBOUR ENERGY PLC", ticker: "HBR", yf: "HBR.L" },
  { label: "EnQuest",        value: "ENQUEST PLC",        ticker: "ENQ", yf: "ENQ.L" },
  { label: "Serica Energy",  value: "SERICA ENERGY",      ticker: "SQZ", yf: "SQZ.L" },
  { label: "Ithaca Energy",  value: "ITHACA ENERGY",      ticker: "ITH", yf: "ITH.L" },
];

const PERIODS = [
  { label: "1Y", days: 365 },
  { label: "2Y", days: 730 },
  { label: "3Y", days: 1095 },
];

const GAS_TICKER   = "TTF=F";
const BRENT_TICKER = "BZ=F";
const YF_BASE      = "https://query1.finance.yahoo.com/v8/finance/chart";

const TOOLTIP_STYLE = {
  background: "#053057",
  border: "1px solid #00EDED40",
  borderRadius: 6,
  color: "#A2F3F3",
  fontSize: 12,
};

function dateKey(ts: number): string {
  return new Date(ts * 1000).toISOString().slice(0, 10);
}

async function fetchYF(symbol: string, days: number): Promise<Map<string, number>> {
  const end   = Math.floor(Date.now() / 1000);
  const start = Math.floor(end - days * 86400);
  try {
    const res = await fetch(
      `${YF_BASE}/${encodeURIComponent(symbol)}?interval=1wk&period1=${start}&period2=${end}`
    );
    if (!res.ok) return new Map();
    const json = await res.json();
    const result = json?.chart?.result?.[0];
    if (!result) return new Map();
    const timestamps: number[]        = result.timestamp ?? [];
    const closes: (number | null)[]   = result.indicators?.quote?.[0]?.close ?? [];
    const map = new Map<string, number>();
    timestamps.forEach((ts, i) => {
      const c = closes[i];
      if (c != null) map.set(dateKey(ts), c);
    });
    return map;
  } catch {
    return new Map();
  }
}

export default function PriceAnalysis() {
  const [operator, setOperator] = useState(OPERATORS[0]);
  const [days, setDays]         = useState(365);
  const [data, setData]         = useState<PricePoint[]>([]);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    setData([]);

    Promise.all([
      fetchYF(operator.yf,  days),
      fetchYF(BRENT_TICKER, days),
      fetchYF(GAS_TICKER,   days),
    ])
      .then(([shareMap, brentMap, gasMap]) => {
        if (shareMap.size === 0) {
          setError(`No data returned for ${operator.yf}`);
          return;
        }

        const raw: PricePoint[] = [];
        for (const [date, share] of shareMap.entries()) {
          raw.push({
            date,
            share,
            brent: brentMap.get(date) ?? null,
            gas:   gasMap.get(date)   ?? null,
          });
        }
        raw.sort((a, b) => a.date.localeCompare(b.date));

        const firstShare = raw[0].share!;
        const firstBrent = raw.find(p => p.brent != null)?.brent ?? 1;
        const firstGas   = raw.find(p => p.gas   != null)?.gas   ?? 1;

        setData(
          raw.map(p => ({
            date:  p.date,
            share: parseFloat(((p.share! / firstShare) * 100).toFixed(1)),
            brent: p.brent != null ? parseFloat(((p.brent / firstBrent) * 100).toFixed(1)) : null,
            gas:   p.gas   != null ? parseFloat(((p.gas   / firstGas)   * 100).toFixed(1)) : null,
          }))
        );
      })
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, [operator, days]);

  const formatTip = (value: number) => {
    const delta = value - 100;
    const sign  = delta >= 0 ? "+" : "";
    return `${value.toFixed(1)}  (${sign}${delta.toFixed(1)}%)`;
  };

  return (
    <div className="space-y-6">
      {/* Controls */}
      <div className="flex flex-wrap gap-4 items-end">
        <div>
          <label className="block text-xs text-[#00EDED] mb-1 uppercase tracking-wider">
            Company
          </label>
          <select
            value={operator.value}
            onChange={e => setOperator(OPERATORS.find(o => o.value === e.target.value)!)}
            className="bg-[#304550] border border-[#00EDED]/30 text-[#A2F3F3] rounded px-3 py-1.5 text-sm focus:outline-none focus:border-[#00EDED]"
          >
            {OPERATORS.map(o => (
              <option key={o.value} value={o.value}>
                {o.label} ({o.ticker})
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-xs text-[#00EDED] mb-1 uppercase tracking-wider">
            Period
          </label>
          <div className="flex gap-1">
            {PERIODS.map(p => (
              <button
                key={p.label}
                onClick={() => setDays(p.days)}
                className={`px-3 py-1.5 text-sm rounded border transition-colors ${
                  days === p.days
                    ? "bg-[#00EDED]/20 border-[#00EDED] text-[#00EDED]"
                    : "bg-[#304550] border-[#00EDED]/20 text-[#A2F3F3] hover:border-[#00EDED]/50"
                }`}
              >
                {p.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      {loading && <p className="text-[#A2F3F3]/50 text-sm">Loading price data...</p>}
      {error   && <p className="text-red-400 text-sm">{error}</p>}

      {!loading && !error && data.length > 0 && (
        <div className="bg-[#304550] rounded-lg p-4 border border-[#00EDED]/15">
          <div className="flex justify-between items-center mb-4">
            <span className="text-[10px] text-[#A2F3F3]/50 uppercase tracking-widest">
              Price Performance
            </span>
            <span className="text-[9px] text-[#A2F3F3]/30">
              All series indexed to 100 at period start
            </span>
          </div>

          <ResponsiveContainer width="100%" height={340}>
            <LineChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#00EDED18" />
              <XAxis
                dataKey="date"
                tick={{ fill: "#A2F3F3", fontSize: 11 }}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fill: "#A2F3F3", fontSize: 11 }}
                tickLine={false}
                axisLine={false}
              />
              <ReferenceLine
                y={100}
                stroke="rgba(255,255,255,0.18)"
                strokeDasharray="4 3"
              />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                formatter={(value: number, name: string) => [formatTip(value), name]}
              />
              <Legend wrapperStyle={{ color: "#A2F3F3", fontSize: 12 }} />

              <Line
                type="monotone"
                dataKey="share"
                name={`${operator.label} (${operator.ticker})`}
                stroke="#22d3ee"
                strokeWidth={2}
                dot={false}
                connectNulls
              />
              <Line
                type="monotone"
                dataKey="brent"
                name="Brent Crude (USD/bbl)"
                stroke="#fb923c"
                strokeWidth={1.5}
                dot={false}
                connectNulls
              />
              <Line
                type="monotone"
                dataKey="gas"
                name="Gas (TTF, EUR/MWh)"
                stroke="#6ee7b7"
                strokeWidth={1.5}
                strokeDasharray="5 3"
                dot={false}
                connectNulls
              />
            </LineChart>
          </ResponsiveContainer>

          <p className="text-[9px] text-[#A2F3F3]/25 text-right mt-2">
            Share price in GBp · Brent: ICE BZ=F · Gas: Dutch TTF (TTF=F)
          </p>
        </div>
      )}
    </div>
  );
}
