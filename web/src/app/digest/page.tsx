import { dbQuery } from "@/lib/databricks";
import BriefingCard from "@/components/BriefingCard";

interface Briefing {
  ticker: string;
  company_name: string;
  year: number;
  briefing: string;
  generated_at: string;
  model: string;
}

async function getBriefings(): Promise<Briefing[]> {
  try {
    return await dbQuery<Briefing>(`
      SELECT ticker, company_name, year, briefing, generated_at, model
      FROM company_briefings
      ORDER BY year DESC, ticker ASC
    `);
  } catch {
    return [];
  }
}

export default async function DigestPage() {
  const briefings = await getBriefings();

  // Group by year, show latest year first
  const byYear = briefings.reduce<Record<number, Briefing[]>>((acc, b) => {
    acc[b.year] = acc[b.year] ?? [];
    acc[b.year].push(b);
    return acc;
  }, {});

  const years = Object.keys(byYear)
    .map(Number)
    .sort((a, b) => b - a);

  if (briefings.length === 0) {
    return (
      <div className="text-center py-24">
        <p className="text-[#A2F3F3]/60 text-lg">No briefings available yet.</p>
        <p className="text-[#A2F3F3]/40 text-sm mt-2">
          Run <code className="font-mono text-[#00EDED]">python -m src.intelligence.briefings</code> to generate the first set.
        </p>
      </div>
    );
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-semibold text-[#00EDED]">Weekly Intelligence Digest</h1>
        <p className="text-[#A2F3F3]/70 text-sm mt-1">
          AI-generated briefings for UKCS-listed operators — production, corporate developments, regulatory context, and commercial outlook.
        </p>
      </div>

      {years.map((year) => (
        <section key={year} className="mb-12">
          <h2 className="text-lg font-medium text-[#A2F3F3] mb-4 border-b border-[#00EDED]/20 pb-2">
            {year}
          </h2>
          <div className="grid gap-6 md:grid-cols-2 xl:grid-cols-3">
            {byYear[year].map((b) => (
              <BriefingCard key={b.ticker} briefing={b} />
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}
