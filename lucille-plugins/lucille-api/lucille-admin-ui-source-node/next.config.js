/** @type {import('next').NextConfig} */
const isProd = process.env.NODE_ENV === 'production' || process.env.EXPORT === 'true';

const nextConfig = {
  output: 'export',
  basePath: isProd ? '/admin' : '',
  assetPrefix: isProd ? '/admin' : '',
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
