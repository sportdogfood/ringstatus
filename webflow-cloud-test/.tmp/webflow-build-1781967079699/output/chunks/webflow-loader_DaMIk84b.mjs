globalThis.process ??= {};
globalThis.process.env ??= {};
function normalizeSrc(src, deployUrl, mountPath) {
  if (deployUrl && src.startsWith(deployUrl)) {
    return `${src.slice(deployUrl.length)}`.slice(1);
  } else if (mountPath && src.startsWith(mountPath)) {
    return src.slice(1);
  } else if (src.startsWith("/")) {
    return `${mountPath}${src}`.slice(1);
  }
  return src;
}
function getTargetDimensions(options) {
  let targetWidth = options.width;
  let targetHeight = options.height;
  if (typeof options.src === "object" && "width" in options.src && "height" in options.src) {
    const aspectRatio = options.src.width / options.src.height;
    if (targetHeight && !targetWidth) {
      targetWidth = Math.round(targetHeight * aspectRatio);
    } else if (targetWidth && !targetHeight) {
      targetHeight = Math.round(targetWidth / aspectRatio);
    } else if (!targetWidth && !targetHeight) {
      targetWidth = options.src.width;
      targetHeight = options.src.height;
    }
  }
  return {
    targetWidth,
    targetHeight
  };
}
const cloudflareLoader = {
  getURL(options, imageConfig) {
    const normalizedSrc = normalizeSrc(
      typeof options.src === "object" ? options.src.src : options.src,
      imageConfig.service.config.deployUrl,
      imageConfig.service.config.mountPath
    );
    if (normalizedSrc.startsWith("http://") || normalizedSrc.startsWith("https://")) {
      return normalizedSrc;
    }
    const supportedOptions = ["width", "height", "quality", "format"];
    const params = [];
    for (const option of supportedOptions) {
      if (options[option]) {
        params.push(`${option}=${options[option]}`);
      }
    }
    const workerUrl = imageConfig.service.config.deployUrl;
    const isSvg = typeof options.src === "object" ? options.src.format === "svg" : options.src.endsWith(".svg");
    if (isSvg || params.length === 0) {
      return `${workerUrl}/${normalizedSrc}`;
    }
    const paramsString = params.join(",");
    return `${workerUrl}/cdn-cgi/image/${paramsString}/${normalizedSrc}`;
  },
  // Default implementation copied from Astro's baseService
  getHTMLAttributes(options) {
    const { targetWidth, targetHeight } = getTargetDimensions(options);
    const {
      src,
      width,
      height,
      format,
      quality,
      densities,
      widths,
      formats,
      layout,
      priority,
      fit,
      position,
      ...attributes
    } = options;
    return {
      ...attributes,
      width: targetWidth,
      height: targetHeight,
      loading: attributes.loading ?? "lazy",
      decoding: attributes.decoding ?? "async"
    };
  }
};
export {
  cloudflareLoader as default
};
