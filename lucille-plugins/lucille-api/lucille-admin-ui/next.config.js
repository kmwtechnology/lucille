/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  // Disable image optimization during development
  images: {
    unoptimized: true,
  },
  eslint: {
    ignoreDuringBuilds: true,
  },
  typescript: {
    ignoreBuildErrors: true,
  },
  experimental: {
    webpackBuildWorker: true,
    parallelServerBuildTraces: true,
    parallelServerCompiles: true,
  },
};

module.exports = nextConfig;
