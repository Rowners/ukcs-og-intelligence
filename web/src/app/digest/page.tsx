import { dbQuery } from "@/lib/databricks";
import BriefingCard from "@/components/BriefingCard";

export const revalidate = 600; // re-fetch from Databricks at most once per 10 minutes

interface Briefing {
  ticker: string;
  company_name: string;
  year: number;
  briefing: string;
  generated_at: string;
  model: string;
}

async function getLatestBriefings(): Promise<Briefing[]> {
  try {
    // One row per ticker — the most recently generated briefing
    return await dbQuery<Briefing>(`
      SELECT ticker, company_name, year, briefing, generated_at, model
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY generated_at DESC) AS rn
        FROM company_briefings
      ) t
      WHERE rn = 1
      ORDER BY ticker ASC
    `);
  } catch {
    return [];
  }
}

function formatDate(ts: string): string {
  return new Date(ts).toLocaleDateString("en-GB", {
    day: "numeric",
    month: "long",
    year: "numeric",
  });
}

export default async function DigestPage() {
  const briefings = await getLatestBriefings();

  const latestUpdate = briefings.length > 0
    ? briefings.reduce((latest, b) =>
        b.generated_at > latest ? b.generated_at : latest,
        briefings[0].generated_at
      )
    : null;

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
          AI-generated briefings for UKCS-listed operators — production, corporate developments,
          regulatory context, and commercial outlook.
        </p>
        {latestUpdate && (
          <p className="text-[#A2F3F3]/40 text-xs mt-2">
            Last updated: {formatDate(latestUpdate)}
          </p>
        )}
      </div>

      <div className="grid gap-6 md:grid-cols-2 xl:grid-cols-3">
        {briefings.map((b) => (
          <BriefingCard key={b.ticker} briefing={b} />
        ))}
      </div>
    </div>
  );
}
