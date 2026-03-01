"use client";

import { useEffect, useState, useMemo } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

interface ProductionRow {
  FIELDNAME: string;
  ORGGRPNM: string;
  PERIODYR: number;
  PERIODMNTH: number;
  OILPRODMBD: number | null;
  AGASPROMMS: number | null;
  DGASPROMMS: number | null;
  GCONDVOL: number | null;
  GASFLARVOL: number | null;
  WATPRODVOL: number | null;
  water_cut_pct: number | null;
}

const OPERATORS = [
  { label: "Harbour Energy", value: "HARBOUR ENERGY PLC" },
  { label: "EnQuest", value: "ENQUEST PLC" },
  { label: "Serica Energy", value: "SERICA ENERGY" },
  { label: "Ithaca Energy", value: "ITHACA ENERGY" },
];

const PROD_METRICS: { key: keyof ProductionRow; label: string; color: string }[] = [
  { key: "OILPRODMBD", label: "Oil (Mbbl)", color: "#22d3ee" },
  { key: "AGASPROMMS", label: "Associated Gas (MMscf)", color: "#6ee7b7" },
  { key: "DGASPROMMS", label: "Gas (MMscf)", color: "#a78bfa" },
];

const ALL_METRIC_KEYS: (keyof ProductionRow)[] = [
  "OILPRODMBD", "AGASPROMMS", "DGASPROMMS", "water_cut_pct",
];


const TOOLTIP_STYLE = {
  background: "#053057",
  border: "1px solid #00EDED40",
  borderRadius: 6,
  color: "#A2F3F3",
  fontSize: 12,
};

function monthLabel(yr: number, mo: number) {
  return `${yr}-${String(mo).padStart(2, "0")}`;
}

export default function ProductionExplorer() {
  const [operator, setOperator] = useState(OPERATORS[0].value);
  const [field, setField] = useState<string>("ALL");
  const [rows, setRows] = useState<ProductionRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeWC, setActiveWC] = useState<number | null>(null);

  // Fetch when operator changes
  useEffect(() => {
    setLoading(true);
    setError(null);
    setField("ALL");

    fetch(`/api/production?operator=${encodeURIComponent(operator)}`)
      .then((r) => r.json())
      .then((data) => {
        if (data.error) throw new Error(data.error);
        setRows(data);
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [operator]);

  // Distinct fields for current operator
  const fields = useMemo(() => {
    const names = Array.from(new Set(rows.map((r) => r.FIELDNAME))).sort();
    return names;
  }, [rows]);

  // Filter rows by selected field
  const filtered = useMemo(() => {
    return field === "ALL" ? rows : rows.filter((r) => r.FIELDNAME === field);
  }, [rows, field]);

  // Aggregate by period (sum volumes; average water cut)
  const chartData = useMemo(() => {
    const byPeriod: Record<string, Record<string, number>> = {};
    const wcCount: Record<string, number> = {};
    for (const r of filtered) {
      const key = monthLabel(r.PERIODYR, r.PERIODMNTH);
      if (!byPeriod[key]) byPeriod[key] = {};
      for (const k of ALL_METRIC_KEYS) {
        const v = r[k] as number | null;
        if (v != null) {
          if (k === "water_cut_pct") {
            byPeriod[key]["water_cut_pct"] = (byPeriod[key]["water_cut_pct"] ?? 0) + v;
            wcCount[key] = (wcCount[key] ?? 0) + 1;
          } else {
            byPeriod[key][k as string] = (byPeriod[key][k as string] ?? 0) + v;
          }
        }
      }
    }
    return Object.entries(byPeriod)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([period, vals]) => ({
        period,
        ...vals,
        // Average WC across fields rather than summing
        ...(vals["water_cut_pct"] != null && wcCount[period]
          ? { water_cut_pct: vals["water_cut_pct"] / wcCount[period] }
          : {}),
      }));
  }, [filtered]);

  // WC badge: show hovered value or fall back to latest data point
  const latestWC = chartData.length > 0
    ? ((chartData[chartData.length - 1] as Record<string, unknown>)["water_cut_pct"] as number | undefined) ?? null
    : null;
  const displayWC = activeWC ?? latestWC;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleChartMouseMove = (state: any) => {
    if (state?.activePayload?.length) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const entry = state.activePayload.find((p: any) => p.dataKey === "water_cut_pct");
      if (entry != null) setActiveWC(entry.value as number);
    }
  };

  return (
    <div className="space-y-6">
      {/* Filters */}
      <div className="flex flex-wrap gap-4">
        <div>
          <label className="block text-xs text-[#00EDED] mb-1 uppercase tracking-wider">Operator</label>
          <select
            value={operator}
            onChange={(e) => setOperator(e.target.value)}
            className="bg-[#304550] border border-[#00EDED]/30 text-[#A2F3F3] rounded px-3 py-1.5 text-sm focus:outline-none focus:border-[#00EDED]"
          >
            {OPERATORS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-xs text-[#00EDED] mb-1 uppercase tracking-wider">Field</label>
          <select
            value={field}
            onChange={(e) => setField(e.target.value)}
            disabled={loading}
            className="bg-[#304550] border border-[#00EDED]/30 text-[#A2F3F3] rounded px-3 py-1.5 text-sm focus:outline-none focus:border-[#00EDED] disabled:opacity-50"
          >
            <option value="ALL">All fields</option>
            {fields.map((f) => (
              <option key={f} value={f}>
                {f}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Charts */}
      {loading && <p className="text-[#A2F3F3]/50 text-sm">Loading...</p>}
      {error && <p className="text-red-400 text-sm">{error}</p>}

      {!loading && !error && chartData.length > 0 && (
        <div
          className="bg-[#304550] rounded-lg p-4 border border-[#00EDED]/15"
          onMouseLeave={() => setActiveWC(null)}
        >
          {/* Panel label */}
          <div className="flex justify-between items-center mb-2">
            <span className="text-[10px] text-[#A2F3F3]/50 uppercase tracking-widest">Production Volumes</span>
            <span className="text-[9px] text-[#A2F3F3]/30">Mbbl / MMscf</span>
          </div>

          {/* Production chart */}
          <ResponsiveContainer width="100%" height={260}>
            <LineChart
              data={chartData}
              margin={{ top: 4, right: 16, left: 0, bottom: 4 }}
              syncId="production-sync"
              onMouseMove={handleChartMouseMove}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#00EDED18" />
              <XAxis
                dataKey="period"
                tick={{ fill: "#A2F3F3", fontSize: 11 }}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis tick={{ fill: "#A2F3F3", fontSize: 11 }} tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                formatter={(value: number) => value.toFixed(1)}
              />
              <Legend wrapperStyle={{ color: "#A2F3F3", fontSize: 12 }} />
              {PROD_METRICS.map((m) => (
                <Line
                  key={m.key as string}
                  type="monotone"
                  dataKey={m.key as string}
                  name={m.label}
                  stroke={m.color}
                  dot={false}
                  strokeWidth={1.5}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>

          {/* Divider with WC badge */}
          <div className="relative border-t border-dashed border-white/10 my-3">
            {displayWC != null && (
              <div className="absolute right-0 -top-[11px] bg-[#fb923c]/15 border border-[#fb923c]/40 rounded px-2 py-px text-[9px] text-[#fb923c] tracking-wider uppercase">
                {activeWC != null ? "WC:" : "Latest WC:"} {displayWC.toFixed(1)}%
              </div>
            )}
          </div>

          {/* Water cut panel label */}
          <div className="flex justify-between items-center mb-2">
            <span className="text-[10px] text-[#A2F3F3]/50 uppercase tracking-widest">Water Cut</span>
            <span className="text-[9px] text-[#A2F3F3]/30">%</span>
          </div>

          {/* Water cut chart */}
          <ResponsiveContainer width="100%" height={120}>
            <LineChart
              data={chartData}
              margin={{ top: 4, right: 16, left: 0, bottom: 4 }}
              syncId="production-sync"
              onMouseMove={handleChartMouseMove}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#00EDED18" />
              <XAxis
                dataKey="period"
                tick={{ fill: "#A2F3F3", fontSize: 11 }}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis tick={{ fill: "#A2F3F3", fontSize: 11 }} tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                formatter={(value: number) => [`${value.toFixed(1)}%`, "Water Cut"]}
              />
              <Line
                type="monotone"
                dataKey="water_cut_pct"
                name="Water Cut (%)"
                stroke="#fb923c"
                dot={false}
                strokeWidth={1.5}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Table */}
      {!loading && !error && filtered.length > 0 && (
        <div className="overflow-x-auto rounded-lg border border-[#00EDED]/15">
          <table className="w-full text-xs text-[#A2F3F3]">
            <thead className="bg-[#003059] text-[#00EDED] uppercase tracking-wider">
              <tr>
                {["Field", "Yr", "Mo", "Oil (Mbbl)", "Assoc Gas (MMscf)", "Gas (MMscf)", "Cond (Mbbl)", "Water Cut %"].map(
                  (h) => (
                    <th key={h} className="px-3 py-2.5 text-left font-medium whitespace-nowrap">
                      {h}
                    </th>
                  )
                )}
              </tr>
            </thead>
            <tbody>
              {filtered.slice(0, 200).map((r, i) => (
                <tr
                  key={i}
                  className={i % 2 === 0 ? "bg-[#304550]" : "bg-[#304550]/60"}
                >
                  <td className="px-3 py-2 font-mono">{r.FIELDNAME}</td>
                  <td className="px-3 py-2">{r.PERIODYR}</td>
                  <td className="px-3 py-2">{r.PERIODMNTH}</td>
                  <td className="px-3 py-2 text-right">{r.OILPRODMBD?.toFixed(2) ?? "-"}</td>
                  <td className="px-3 py-2 text-right">{r.AGASPROMMS?.toFixed(2) ?? "-"}</td>
                  <td className="px-3 py-2 text-right">{r.DGASPROMMS?.toFixed(2) ?? "-"}</td>
                  <td className="px-3 py-2 text-right">{r.GCONDVOL?.toFixed(2) ?? "-"}</td>
                  <td className="px-3 py-2 text-right">
                    {r.water_cut_pct != null ? `${r.water_cut_pct}%` : "-"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {filtered.length > 200 && (
            <p className="text-[#A2F3F3]/40 text-xs px-3 py-2">
              Showing 200 of {filtered.length} rows. Filter by field to see more.
            </p>
          )}
        </div>
      )}
    </div>
  );
}
