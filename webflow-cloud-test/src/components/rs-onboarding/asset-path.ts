// Preserve the same assets under the Webflow Cloud mount (including a trailing slash).
export function assetPath(path: string): string {
  if (typeof window === 'undefined') return path;
  const mount = window.location.pathname.replace(/\/onboarding\/?$/, '');
  return `${mount === window.location.pathname ? '' : mount}${path}`;
}
