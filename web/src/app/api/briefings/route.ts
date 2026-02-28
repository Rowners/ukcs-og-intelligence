import { NextRequest, NextResponse } from "next/server";
import { dbQuery } from "@/lib/databricks";

export interface Briefing {
  ticker: string;
  company_name: string;
  year: number;
  briefing: string;
  generated_at: string;
  model: string;
}

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const ticker = searchParams.get("ticker");

  try {
    let sql = `
      SELECT ticker, company_name, year, briefing, generated_at, model
      FROM company_briefings
    `;
    const params: string[] = [];

    if (ticker) {
      sql += " WHERE UPPER(ticker) = UPPER(?)";
      params.push(ticker);
    }

    sql += " ORDER BY year DESC, ticker ASC";

    const rows = await dbQuery<Briefing>(sql, params);
    return NextResponse.json(rows);
  } catch (err) {
    console.error("Briefings API error:", err);
    return NextResponse.json({ error: "Failed to load briefings" }, { status: 500 });
  }
}
