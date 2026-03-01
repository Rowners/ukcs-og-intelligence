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
  { label: "Harbour Energy", value: "HARBOUR ENERGY PLC", ticker: "HBR" },
  { label: "EnQuest",        value: "ENQUEST PLC",        ticker: "ENQ" },
  { label: "Serica Energy",  value: "SERICA ENERGY",      ticker: "SQZ" },
  { label: "Ithaca Energy",  value: "ITHACA ENERGY",      ticker: "ITH" },
];

const PERIODS = [
  { label: "1Y", days: 365 },
  { label: "2Y", days: 730 },
  { label: "3Y", days: 1095 },
];

const TOOLTIP_STYLE = {
  background: "#053057",
  border: "1px solid #00EDED40",
  borderRadius: 6,
  color: "#A2F3F3",
  fontSize: 12,
};

export default function PriceAnalysis() {
  const [operator, setOperator] = useState(OPERATORS[0].value);
  const [days, setDays]         = useState(365);
  const [data, setData]         = useState<PricePoint[]>([]);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    setData([]);
    fetch(`/api/prices?operator=${encodeURIComponent(operator)}&days=${days}`)
      .then(r => r.json())
      .then(d => {
        if (d.error) throw new Error(d.error);
        setData(d);
      })
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, [operator, days]);

  const selectedOp = OPERATORS.find(o => o.value === operator)!;

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
            value={operator}
            onChange={e => setOperator(e.target.value)}
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
                name={`${selectedOp.label} (${selectedOp.ticker})`}
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
