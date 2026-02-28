import ProductionExplorer from "@/components/ProductionExplorer";

export default function ExplorerPage() {
  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-[#00EDED]">Data Explorer</h1>
        <p className="text-[#A2F3F3]/70 text-sm mt-1">
          Interactive view of UKCS field production data by operator and field.
        </p>
      </div>
      <ProductionExplorer />
    </div>
  );
}
