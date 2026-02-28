import Link from "next/link";

export default function Nav() {
  return (
    <nav className="bg-[#003059] border-b border-[#00EDED]/20">
      <div className="max-w-7xl mx-auto px-4 h-14 flex items-center justify-between">
        <Link href="/" className="text-[#00EDED] font-semibold tracking-wide text-sm uppercase">
          UKCS Intelligence
        </Link>
        <div className="flex gap-6 text-sm">
          <Link
            href="/digest"
            className="text-[#A2F3F3] hover:text-[#00EDED] transition-colors"
          >
            Weekly Digest
          </Link>
          <Link
            href="/explorer"
            className="text-[#A2F3F3] hover:text-[#00EDED] transition-colors"
          >
            Data Explorer
          </Link>
        </div>
      </div>
    </nav>
  );
}
