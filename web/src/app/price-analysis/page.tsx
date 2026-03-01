import PriceAnalysis from "@/components/PriceAnalysis";

export default function PriceAnalysisPage() {
  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-[#00EDED]">Price Analysis</h1>
        <p className="text-[#A2F3F3]/70 text-sm mt-1">
          Share price performance indexed against Brent crude and TTF gas prices.
        </p>
      </div>
      <PriceAnalysis />
    </div>
  );
}
