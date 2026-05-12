const { chromium } = require('playwright');
const path = require('path');

const viewports = [
  [1280, 720],
  [1366, 768],
  [1440, 900],
  [1440, 1000],
  [1536, 864],
  [1920, 1080],
  [1024, 768],
  [992, 720],
  [768, 1024],
  [390, 844],
];

(async () => {
  const browser = await chromium.launch({ headless: true });
  const fileUrl = 'file:///' + path.resolve('tmp/rs185-single.html').replace(/\\/g, '/');

  for (const [width, height] of viewports) {
    const page = await browser.newPage({ viewport: { width, height }, deviceScaleFactor: 1 });
    await page.goto(fileUrl, { waitUntil: 'networkidle' });
    await page.screenshot({
      path: path.resolve(`tmp/rs185-${width}x${height}.png`),
      fullPage: false,
    });
    await page.close();
  }

  await browser.close();
  console.log(viewports.map(([w, h]) => path.resolve(`tmp/rs185-${w}x${h}.png`)).join('\n'));
})();
