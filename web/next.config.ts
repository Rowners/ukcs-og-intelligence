import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Server-side Databricks connector — exclude from client bundle
  serverExternalPackages: ["@databricks/sql"],
};

export default nextConfig;
