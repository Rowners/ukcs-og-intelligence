import { dbQuery, describeDbError } from "@/lib/databricks";
import BriefingCard from "@/components/BriefingCard";

export const revalidate = 120;
export const maxDuration = 30;

interface Briefing {
  ticker: string;
  company_name: string;
  year: number;
  briefing: string;
  generated_at: string;
  model: string;
}

async function getLatestBriefings(): Promise<{ briefings: Briefing[]; error: string | null }> {
  try {
    // One row per ticker — the most recently generated briefing
    const briefings = await dbQuery<Briefing>(`
      SELECT ticker, company_name, year, briefing, generated_at, model
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY generated_at DESC) AS rn
        FROM company_briefings
      ) t
      WHERE rn = 1
      ORDER BY ticker ASC
    `);
    return { briefings, error: null };
  } catch (err) {
    console.error("Digest Databricks query failed:", err);
    return { briefings: [], error: describeDbError(err) };
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
  const { briefings, error } = await getLatestBriefings();

  if (error) {
    return (
      <div className="max-w-2xl mx-auto py-24 text-center">
        <p className="text-[#00EDED] text-lg">Briefings could not be loaded.</p>
        <p className="text-[#A2F3F3]/70 text-sm mt-3 leading-relaxed">
          This page reads the Databricks table <span className="font-mono text-[#A2F3F3]">company_briefings</span>.
          The query failed. Start the SQL warehouse, then check the Databricks environment
          variables on Vercel (<span className="font-mono">DATABRICKS_SERVER_HOSTNAME</span>,{" "}
          <span className="font-mono">DATABRICKS_HTTP_PATH</span>,{" "}
          <span className="font-mono">DATABRICKS_ACCESS_TOKEN</span>).
        </p>
        <p className="text-[#A2F3F3]/50 text-xs mt-4 font-mono break-words">{error}</p>
      </div>
    );
  }

  const latestUpdate = briefings.length > 0
    ? briefings.reduce((latest, b) =>
        b.generated_at > latest ? b.generated_at : latest,
        briefings[0].generated_at
      )
    : null;

  if (briefings.length === 0) {
    return (
      <div className="text-center py-24">
        <p className="text-[#A2F3F3]/60 text-lg">Databricks returned no briefings.</p>
        <p className="text-[#A2F3F3]/40 text-sm mt-2 max-w-lg mx-auto leading-relaxed">
          The <span className="font-mono text-[#A2F3F3]/70">company_briefings</span> table is empty.
          Run the daily pipeline (<span className="font-mono text-[#00EDED]">python -m src.scheduler --job daily</span>,
          or the Daily pipeline workflow) after the SQL warehouse is started.
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
