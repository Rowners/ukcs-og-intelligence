"use client";

import { useState } from "react";

interface Briefing {
  ticker: string;
  company_name: string;
  year: number;
  briefing: string;
  generated_at: string;
  model: string;
}

const SECTION_HEADINGS = [
  "Production Performance",
  "Corporate Developments",
  "Regulatory Context",
  "Commercial Outlook",
];

function parseSections(text: string): { heading: string; body: string }[] {
  const sections: { heading: string; body: string }[] = [];

  for (let i = 0; i < SECTION_HEADINGS.length; i++) {
    const heading = SECTION_HEADINGS[i];
    const next = SECTION_HEADINGS[i + 1];
    const re = new RegExp(
      `(?:#{1,3}\\s*\\d*\\.?\\s*)?${heading}[:\\s]*([\\s\\S]*?)${next ? `(?=(?:#{1,3}\\s*\\d*\\.?\\s*)?${next})` : "$"}`,
      "i"
    );
    const match = text.match(re);
    if (match) {
      sections.push({ heading, body: match[1].trim() });
    }
  }

  if (sections.length === 0) {
    return [{ heading: "Briefing", body: text }];
  }

  return sections;
}

function generatedAgo(ts: string): string {
  const d = new Date(ts);
  const diff = Date.now() - d.getTime();
  const hours = Math.floor(diff / 3_600_000);
  if (hours < 1) return "< 1h ago";
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export default function BriefingCard({ briefing }: { briefing: Briefing }) {
  const [expanded, setExpanded] = useState(false);
  const sections = parseSections(briefing.briefing);

  return (
    <div className="bg-[#304550] rounded-lg border border-[#00EDED]/15 overflow-hidden flex flex-col">
      {/* Header */}
      <div className="bg-[#003059] px-5 py-4 flex items-center justify-between">
        <div>
          <span className="text-[#00EDED] font-semibold text-sm font-mono">{briefing.ticker}</span>
          <h3 className="text-[#A2F3F3] font-medium text-base leading-tight mt-0.5">
            {briefing.company_name}
          </h3>
        </div>
        <span className="text-[#A2F3F3]/40 text-xs">{generatedAgo(briefing.generated_at)}</span>
      </div>

      {/* Content */}
      <div className="px-5 py-4 flex-1 text-sm text-[#A2F3F3]/85 space-y-4">
        {(expanded ? sections : sections.slice(0, 1)).map((s) => (
          <div key={s.heading}>
            <p className="text-[#00EDED] text-xs font-semibold uppercase tracking-wider mb-1">
              {s.heading}
            </p>
            <p className="leading-relaxed whitespace-pre-line">{s.body}</p>
          </div>
        ))}
      </div>

      {/* Expand toggle */}
      <button
        onClick={() => setExpanded((v) => !v)}
        className="w-full text-center py-2.5 text-xs text-[#00EDED] hover:bg-[#00EDED]/10 transition-colors border-t border-[#00EDED]/15"
      >
        {expanded ? "Show less" : `Show all ${sections.length} sections`}
      </button>
    </div>
  );
}
