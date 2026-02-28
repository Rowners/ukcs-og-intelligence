/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    // Next.js 14: keep @databricks/sql server-side only, out of webpack bundle
    serverComponentsExternalPackages: ["@databricks/sql"],
  },
};

export default nextConfig;
