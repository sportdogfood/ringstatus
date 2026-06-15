// Merge logic for the generated astro.config.mjs (shared between Astro 5 + 6).
// Copied verbatim into the user's project; types are local to avoid Astro
// cross-version drift.

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AstroIntegration = { name?: string; [key: string]: any };
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AstroUserConfig = Record<string, any>;

export type WebflowOverridesOptions = {
  mountPath: string;
  deployUrl: string;
  /** The result of `cloudflare({ ... })` from `@astrojs/cloudflare`. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  adapter: any;
  /** The result of `react()` from `@astrojs/react`. Auto-added if the user hasn't already added `@astrojs/react`. */
  reactIntegration: AstroIntegration;
  /** Extra Vite resolve.alias to inject (e.g. react-dom/server -> react-dom/server.edge for React 19+ in prod). Pass `undefined` to skip. */
  reactDomServerAlias: Record<string, string> | undefined;
};

/**
 * Wrap the user's Astro config with Webflow Cloud's required overrides.
 * `userExport` is always provided — `{}` when the user ships no astro.config.*.
 *
 * Matches pre-1.2 behaviour: top-level keys in the override block REPLACE the
 * user's. We need our adapter / base / output to win.
 */
export function withWebflowOverrides(
  userExport: AstroUserConfig,
  opts: WebflowOverridesOptions
): AstroUserConfig {
  const userIntegrations: AstroIntegration[] = userExport.integrations || [];
  const hasReact = userIntegrations.some((i) => i?.name === "@astrojs/react");
  const integrations = hasReact
    ? userIntegrations
    : [...userIntegrations, opts.reactIntegration];

  return {
    ...userExport,
    base: opts.mountPath,
    output: "server",
    adapter: opts.adapter,
    integrations,
    vite: {
      ...userExport.vite,
      resolve: {
        ...userExport.vite?.resolve,
        alias: opts.reactDomServerAlias,
      },
    },
    build: {
      assetsPrefix:
        opts.deployUrl + (opts.mountPath === "/" ? "" : opts.mountPath),
    },
    image: {
      ...userExport.image,
      service: {
        entrypoint: "./webflow-loader.ts",
        config: {
          deployUrl: opts.deployUrl,
          mountPath: opts.mountPath,
        },
      },
    },
  };
}
