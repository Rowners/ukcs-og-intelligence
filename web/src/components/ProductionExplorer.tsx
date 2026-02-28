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
  { label: "Tullow", value: "TULLOW UK" },
  { label: "Afentra", value: "ADURA ENERGY" },
  { label: "Gulf Keystone", value: "GULF KEYSTONE" },
  { label: "Kistos", value: "KISTOS" },
  { label: "Jersey Oil & Gas", value: "JERSEY OIL AND GAS" },
];

const METRICS: { key: keyof ProductionRow; label: string; color: string }[] = [
  { key: "OILPRODMBD", label: "Oil (Mbbl)", color: "#00EDED" },
  { key: "AGASPROMMS", label: "Gas-Assoc (MMscf)", color: "#A2F3F3" },
  { key: "DGASPROMMS", label: "Gas-Disassoc (MMscf)", color: "#7dd3fc" },
  { key: "water_cut_pct", label: "Water Cut (%)", color: "#fb923c" },
];

function monthLabel(yr: number, mo: number) {
  return `${yr}-${String(mo).padStart(2, "0")}`;
}

export default function ProductionExplorer() {
  const [operator, setOperator] = useState(OPERATORS[0].value);
  const [field, setField] = useState<string>("ALL");
  const [rows, setRows] = useState<ProductionRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

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
    const names = [...new Set(rows.map((r) => r.FIELDNAME))].sort();
    return names;
  }, [rows]);

  // Filter rows by selected field
  const filtered = useMemo(() => {
    return field === "ALL" ? rows : rows.filter((r) => r.FIELDNAME === field);
  }, [rows, field]);

  // Aggregate by period (sum across fields)
  const chartData = useMemo(() => {
    const byPeriod: Record<string, Record<string, number>> = {};
    for (const r of filtered) {
      const key = monthLabel(r.PERIODYR, r.PERIODMNTH);
      if (!byPeriod[key]) byPeriod[key] = {};
      for (const m of METRICS) {
        const v = r[m.key] as number | null;
        if (v != null) {
          byPeriod[key][m.key] = (byPeriod[key][m.key] ?? 0) + v;
        }
      }
    }
    return Object.entries(byPeriod)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([period, vals]) => ({ period, ...vals }));
  }, [filtered]);

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

      {/* Chart */}
      {loading && <p className="text-[#A2F3F3]/50 text-sm">Loading...</p>}
      {error && <p className="text-red-400 text-sm">{error}</p>}

      {!loading && !error && chartData.length > 0 && (
        <div className="bg-[#304550] rounded-lg p-4 border border-[#00EDED]/15">
          <ResponsiveContainer width="100%" height={360}>
            <LineChart data={chartData} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#00EDED18" />
              <XAxis
                dataKey="period"
                tick={{ fill: "#A2F3F3", fontSize: 11 }}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis tick={{ fill: "#A2F3F3", fontSize: 11 }} tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={{
                  background: "#053057",
                  border: "1px solid #00EDED40",
                  borderRadius: 6,
                  color: "#A2F3F3",
                  fontSize: 12,
                }}
              />
              <Legend wrapperStyle={{ color: "#A2F3F3", fontSize: 12 }} />
              {METRICS.map((m) => (
                <Line
                  key={m.key}
                  type="monotone"
                  dataKey={m.key}
                  name={m.label}
                  stroke={m.color}
                  dot={false}
                  strokeWidth={1.5}
                />
              ))}
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
                {["Field", "Yr", "Mo", "Oil (Mbbl)", "Gas-A (MMscf)", "Gas-D (MMscf)", "Cond (Mbbl)", "Water Cut %"].map(
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
