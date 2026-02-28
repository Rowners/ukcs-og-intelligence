import { NextRequest, NextResponse } from "next/server";
import { dbQuery } from "@/lib/databricks";

export interface ProductionRow {
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
  // computed
  water_cut_pct: number | null;
}

const M3_TO_MBBL = 6.2898 / 1000;

function computeWaterCut(row: ProductionRow): number | null {
  const oil = row.OILPRODMBD ?? 0;
  const watMbbl = (row.WATPRODVOL ?? 0) * M3_TO_MBBL;
  const total = oil + watMbbl;
  if (total <= 0) return null;
  return parseFloat(((watMbbl / total) * 100).toFixed(1));
}

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const operator = searchParams.get("operator");
  const field = searchParams.get("field");
  const year = searchParams.get("year");

  try {
    let sql = `
      SELECT FIELDNAME, ORGGRPNM, PERIODYR, PERIODMNTH,
             OILPRODMBD, AGASPROMMS, DGASPROMMS, GCONDVOL, GASFLARVOL, WATPRODVOL
      FROM nsta_field_production_raw
      WHERE 1=1
    `;
    const params: (string | number)[] = [];

    if (operator) {
      sql += " AND UPPER(ORGGRPNM) = UPPER(?)";
      params.push(operator);
    }
    if (field) {
      sql += " AND UPPER(FIELDNAME) = UPPER(?)";
      params.push(field);
    }
    if (year) {
      sql += " AND PERIODYR = ?";
      params.push(parseInt(year));
    }

    sql += " ORDER BY PERIODYR DESC, PERIODMNTH DESC, FIELDNAME ASC";
    sql += " LIMIT 5000";

    const rows = await dbQuery<ProductionRow>(sql, params);
    const enriched = rows.map((r) => ({ ...r, water_cut_pct: computeWaterCut(r) }));

    return NextResponse.json(enriched);
  } catch (err) {
    console.error("Production API error:", err);
    return NextResponse.json({ error: "Failed to load production data" }, { status: 500 });
  }
}
