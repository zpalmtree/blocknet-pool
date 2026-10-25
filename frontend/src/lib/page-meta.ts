import pages from './page-meta.json' with { type: 'json' };

/** Ignore language/lookup queries and normalize the same paths as the server. */
export function metadataForPath(pathname: string, baseUrl: string) {
  const path = pathname.replace(/\/+$/, '') || '/';
  const meta = Object.prototype.hasOwnProperty.call(pages, path) ? pages[path as keyof typeof pages] : pages['/404'];
  return {
    ...meta,
    canonical: `${baseUrl}${path}`,
    imageUrl: `${baseUrl}/ui-assets/${meta.image}`,
    robots: meta.index ? 'index, follow' : 'noindex, follow',
  };
}

/** Keep route changes, including Back/Forward and recovery from 404, in sync. */
export function updatePageMetadata(pathname: string, title: string) {
  const canonical = document.querySelector<HTMLLinkElement>('link[rel="canonical"]');
  if (!canonical) return;
  // Preserve the server-configured public origin, never a preview/request host.
  const meta = metadataForPath(pathname, new URL(canonical.href).origin);
  canonical.href = meta.canonical;
  document.title = title;
  const fields = {
    'name:description': meta.description,
    'name:robots': meta.robots,
    'property:og:url': meta.canonical,
    'property:og:title': title,
    'property:og:description': meta.description,
    'property:og:image': meta.imageUrl,
    'name:twitter:title': title,
    'name:twitter:description': meta.description,
    'name:twitter:image': meta.imageUrl,
  };
  for (const [field, content] of Object.entries(fields)) {
    const split = field.indexOf(':');
    const attr = field.slice(0, split), value = field.slice(split + 1);
    document.querySelector<HTMLMetaElement>(`meta[${attr}="${value}"]`)?.setAttribute('content', content);
  }
}
