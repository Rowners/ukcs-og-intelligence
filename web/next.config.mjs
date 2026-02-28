/** @type {import('next').NextConfig} */
const nextConfig = {
  // Server-side Databricks connector — exclude from client bundle
  serverExternalPackages: ["@databricks/sql"],
};

export default nextConfig;
